/**
 * Copyright 2013 Daniel Valcarce Silva
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.udc.fi.dc.irlab.nmf.hcomputation;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.nmf.common.MappingsReducer;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;

/**
 * Emit &lt;j, a_j · x_j / y_j> from &lt;j, {a_j, x_j, y_j}>.
 */
public class HComputationReducer
        extends MappingsReducer<IntPairWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    protected void reduce(final IntPairWritable key, final Iterable<VectorWritable> values,
            final Context context) throws IOException, InterruptedException {

        Vector vectorX, vectorH, vectorY;
        final Iterator<VectorWritable> it = values.iterator();
        try {
            vectorH = it.next().get();
            vectorX = it.next().get();
            vectorY = it.next().get();
        } catch (final NoSuchElementException e) {
            throw new NoSuchElementException(
                    String.format("User %d has not rated any item", key.getFirst()));
        }

        // XY = X ./ (Y + eps)
        final Vector vectorXY = vectorX.assign(vectorY, new DoubleDoubleFunction() {
            @Override
            public double apply(final double a, final double b) {
                return a / (b + MatrixComputationJob.eps);
            }
        });

        final Configuration conf = context.getConfiguration();
        final int iteration = conf.getInt(RMRecommenderDriver.iteration, -1);
        final int numberOfIterations = conf.getInt(RMRecommenderDriver.numberOfIterations, -1);
        int userId;

        if (iteration < numberOfIterations) {
            userId = key.getFirst();
        } else {
            userId = getOldUserId(key.getFirst());
        }

        context.write(new IntWritable(userId), new VectorWritable(vectorH.times(vectorXY)));

    }

}
