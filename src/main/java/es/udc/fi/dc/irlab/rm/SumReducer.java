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
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Emit <x, sum_y A_xy> from <x, {A_xy}>.
 */
public class SumReducer extends
	Reducer<Writable, FloatWritable, Writable, FloatWritable> {

    @Override
    protected void reduce(Writable key, Iterable<FloatWritable> values,
	    Context context) throws IOException, InterruptedException {

	Iterator<FloatWritable> it = values.iterator();

	float sum = it.next().get();
	while (it.hasNext()) {
	    sum += it.next().get();
	}

	context.write(key, new FloatWritable(sum));

    }

}
