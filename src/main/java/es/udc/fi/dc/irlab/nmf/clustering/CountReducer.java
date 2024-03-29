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

package es.udc.fi.dc.irlab.nmf.clustering;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Count number of values for each key.
 */
public class CountReducer extends Reducer<Writable, Writable, Writable, IntWritable> {

    @Override
    protected void reduce(final Writable key, final Iterable<Writable> values,
            final Context context) throws IOException, InterruptedException {

        final Iterator<Writable> it = values.iterator();

        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }

        context.write(key, new IntWritable(count));

    }

}
