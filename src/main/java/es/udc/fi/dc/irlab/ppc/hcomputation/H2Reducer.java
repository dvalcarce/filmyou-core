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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit <j,x_j> from <j, {A_{i,j}w_i^T}> where x_j = sum_i(A_{i,j}w_i^T)
 * 
 */
public class H2Reducer extends
	Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<VectorWritable> values,
	    Context context) throws IOException, InterruptedException {
	Iterator<VectorWritable> it = values.iterator();
	Vector output = it.next().get();

	while (it.hasNext()) {
	    output = it.next().get().plus(output);
	}
	context.write(key, new VectorWritable(output));
    }

}
