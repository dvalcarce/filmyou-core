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
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit <(j, 2), y_j> from Y matrix ({y_j}).
 */
public class H5YMapper extends
	Mapper<IntWritable, VectorWritable, IntPairWritable, VectorWritable> {

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
	    throws IOException, InterruptedException {

	context.write(new IntPairWritable(key.get(), 2), value);

    }

}