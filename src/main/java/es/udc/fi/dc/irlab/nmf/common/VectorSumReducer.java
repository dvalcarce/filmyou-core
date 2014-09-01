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

package es.udc.fi.dc.irlab.nmf.common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.util.MahoutMathUtils;

/**
 * Emit &lt;i,a_i> from &lt;i, {b_i}> where a_i = sum_i(b_i).
 * 
 */
public class VectorSumReducer extends
		Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<VectorWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<VectorWritable> it = values.iterator();
		Vector output = it.next().get();

		while (it.hasNext()) {
			// output = it.next().get().plus(output);
			MahoutMathUtils.vectorAddInPlace(output, it.next().get());
		}
		context.write(new IntWritable((int) key.get()), new VectorWritable(
				output));
	}

}
