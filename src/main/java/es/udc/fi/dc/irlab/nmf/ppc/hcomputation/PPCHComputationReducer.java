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

package es.udc.fi.dc.irlab.nmf.ppc.hcomputation;

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
 * Emits <j^(old), h_j^(new)> from <j^(new), {h_j, x_j, y_j}>.
 *
 * h_j^(new) = h_j .* (x_j + d_j) ./ (y_j + e_j)
 */
public class PPCHComputationReducer
        extends MappingsReducer<IntPairWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    protected void reduce(final IntPairWritable key, final Iterable<VectorWritable> values,
            final Context context) throws IOException, InterruptedException {

        Vector result, vectorX, vectorH, vectorY;
        double d_j, e_j;
        final Iterator<VectorWritable> it = values.iterator();

        try {
            vectorH = it.next().get();
            vectorX = it.next().get();
            vectorY = it.next().get();
        } catch (final NoSuchElementException e) {
            throw new NoSuchElementException(
                    String.format("User %d has not rated any item", key.getFirst()));
        }

        // Calculate diagonal elements
        d_j = vectorH.dot(vectorY);
        e_j = vectorH.dot(vectorX);

        // X = X + d_j
        vectorX = vectorX.plus(d_j);

        // Y = Y + e_j
        vectorY = vectorY.plus(e_j);

        // XY = X ./ (Y + eps)
        final Vector vectorXY = vectorX.assign(vectorY, new DoubleDoubleFunction() {
            @Override
            public double apply(double a, double b) {
                if (Double.isInfinite(a)) {
                    a = Double.MAX_VALUE;
                }
                if (Double.isInfinite(b)) {
                    b = Double.MAX_VALUE;
                }
                return a / (b + MatrixComputationJob.eps);
            }
        });

        // H = H .* XY
        result = vectorH.times(vectorXY);

        // Enforce the constraints
        final Configuration conf = context.getConfiguration();
        final int iteration = conf.getInt(RMRecommenderDriver.iteration, -1);
        final int numberOfIterations = conf.getInt(RMRecommenderDriver.numberOfIterations, -1);
        final int normalizationFrequency = conf.getInt(RMRecommenderDriver.normalizationFrequency,
                -1);
        if (iteration % normalizationFrequency == 0) {
            result.normalize(1);
        }
        int userId;
        if (iteration < numberOfIterations) {
            userId = key.getFirst();
        } else {
            userId = getOldUserId(key.getFirst());
        }
        context.write(new IntWritable(userId), new VectorWritable(result));

    }

}
