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

import es.udc.fi.dc.irlab.common.AbstractByClusterAndCountMapper;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.StringIntPairWritable;

/**
 * Emit &lt;(k, 1), (j, sum_i A_{i,j})> from &lt;j, sum_i A_{i,j}> where j is a
 * user from the cluster k.
 */
public class UserSumByClusterMapper extends
        AbstractByClusterAndCountMapper<IntWritable, DoubleWritable, StringIntPairWritable, IntDoubleOrPrefWritable> {

    @Override
    protected void map(final IntWritable key, final DoubleWritable column, final Context context)
            throws IOException, InterruptedException {

        for (final String split : getSplits(key.get())) {
            context.write(new StringIntPairWritable(split, 0),
                    new IntDoubleOrPrefWritable(key.get(), column.get()));
        }

    }

}
