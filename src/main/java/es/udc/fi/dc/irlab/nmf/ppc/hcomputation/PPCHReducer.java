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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;

/**
 * Emits <j, h_j^(new)> from <j, {h_j, x_j, y_j}>.
 * 
 * h_j^(new) = h_j .* (x_j + d_j) ./ (y_j + e_j)
 */
public class PPCHReducer extends
	Reducer<IntPairWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    protected void reduce(IntPairWritable key, Iterable<VectorWritable> values,
	    Context context) throws IOException, InterruptedException {

	Vector result;

	Iterator<VectorWritable> it = values.iterator();
	Vector vectorH = it.next().get();
	Vector vectorX = it.next().get();
	Vector vectorY = it.next().get();

	double d_j = vectorH.dot(vectorY);
	double e_j = vectorH.dot(vectorX);

	// X = X + d_j
	vectorX = vectorX.plus(d_j);

	// Y = Y + e_j
	vectorY = vectorY.plus(e_j);

	// XY = X ./ Y
	Vector vectorXY = vectorX.assign(vectorY, new DoubleDoubleFunction() {
	    public double apply(double a, double b) {
		return a / b;
	    }
	});

	// H = H .* XY
	result = vectorH.times(vectorXY);
	if (context.getConfiguration().getInt("iteration", -1)
		% PPCComputeHJob.normalizationFrequency == 0) {
	    result.normalize(1);
	}
	context.write(new IntWritable(key.getFirst()), new VectorWritable(
		result));

    }

}
