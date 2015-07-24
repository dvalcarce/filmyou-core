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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Emit <x, sum_y A_xy> from <x, {A_xy}>.
 */
public class DoubleSumAndCountReducer
        extends Reducer<Writable, DoubleWritable, Writable, DoubleWritable> {

    @Override
    protected void reduce(final Writable key, final Iterable<DoubleWritable> values,
            final Context context) throws IOException, InterruptedException {

        double sum = 0;

        for (final DoubleWritable val : values) {
            sum += val.get();
        }

        context.getCounter(RM2Job.Score.SUM).increment((long) sum * RM2Job.OFFSET);

        context.write(key, new DoubleWritable(sum));

    }

}
