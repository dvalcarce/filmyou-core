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

package es.udc.fi.dc.irlab.ppc.hcomputation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;

/**
 * Emit <j, h_j · x_j / y_j> from <j, {h_j, x_j, y_j}>.
 */
public class H5Reducer extends
	Reducer<IntPairWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    protected void reduce(IntPairWritable key, Iterable<VectorWritable> values,
	    Context context) throws IOException, InterruptedException {

	Iterator<VectorWritable> it = values.iterator();
	Vector vectorH = it.next().get();
	Vector vectorX = it.next().get();
	Vector vectorY = it.next().get();

	Vector vectorXY = vectorX.assign(vectorY, new DoubleDoubleFunction() {
	    public double apply(double a, double b) {
		return a / b;
	    }
	});

	context.write(new IntWritable(key.getFirst()), new VectorWritable(
		vectorH.times(vectorXY)));

    }
}
