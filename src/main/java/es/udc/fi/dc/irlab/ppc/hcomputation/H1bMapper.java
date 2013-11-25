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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit <(i+1, 0), w_i> from H matrix ({w_i}).
 */
public class H1bMapper
	extends
	Mapper<IntWritable, VectorWritable, IntPairWritable, VectorOrPrefWritable> {

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
	    throws IOException, InterruptedException {

	int item = key.get() + 1;
	Vector vector = value.get();
	context.write(new IntPairWritable(item, 0), new VectorOrPrefWritable(
		vector));
    }
}
