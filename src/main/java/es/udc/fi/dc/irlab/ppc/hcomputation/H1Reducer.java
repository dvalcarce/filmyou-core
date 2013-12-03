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
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit <j, A_{i,j}w_i^T> from <i, {w_i, {(j, A_{i, j}})>.
 */
public class H1Reducer
	extends
	Reducer<IntPairWritable, VectorOrPrefWritable, LongWritable, VectorWritable> {

    @Override
    protected void reduce(IntPairWritable key,
	    Iterable<VectorOrPrefWritable> values, Context context)
	    throws IOException, InterruptedException {

	long user;
	double score;
	VectorWritable output;

	Iterator<VectorOrPrefWritable> it = values.iterator();
	Vector vector = it.next().getVector();

	if (vector == null) {
	    throw new IllegalArgumentException("Bad reduce-side join ("
		    + getClass() + ")");
	}
	while (it.hasNext()) {
	    VectorOrPrefWritable pref = it.next();
	    user = pref.getUserID();
	    score = pref.getValue();
	    if (score > 0) {
		output = new VectorWritable(vector.times(score));
		context.write(new LongWritable(user), output);
	    }
	}

    }

}
