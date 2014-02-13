/**
 * Copyright 2014 Daniel Valcarce Silva
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

package es.udc.fi.dc.irlab.rm;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Emit &lt;NULL, x> from &lt;i, x> where x = sum_j A_{i, j}.
 */
public class NullMapper extends
	Mapper<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {

    @Override
    protected void map(IntWritable key, DoubleWritable value, Context context)
	    throws IOException, InterruptedException {

	context.write(NullWritable.get(), value);

    }

}
