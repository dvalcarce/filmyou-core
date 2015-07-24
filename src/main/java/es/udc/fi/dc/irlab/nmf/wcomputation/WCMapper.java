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

package es.udc.fi.dc.irlab.nmf.wcomputation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.common.AbstractDistributedMatrixMapper;

/**
 * Emit &lt;i, y_i> from &lt;i, w_i> where y_i = w_i·C.
 *
 */
public class WCMapper extends AbstractDistributedMatrixMapper {

    @Override
    protected void map(final IntWritable key, final VectorWritable value, final Context context)
            throws IOException, InterruptedException {

        final Vector vector = value.get();
        final Matrix mVector = new DenseMatrix(1, vector.size());

        for (int j = 0; j < vector.size(); j++) {
            mVector.set(0, j, vector.get(j));
        }

        context.write(key, new VectorWritable(mVector.times(C).viewRow(0)));

    }

}
