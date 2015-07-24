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

package es.udc.fi.dc.irlab.nmf.hcomputation;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.common.AbstractJoinVectorMapper;

/**
 * Emit &lt;j, A_{i,j}w_i^T)> from HDFS ratings (&lt;(j, i), A_{i,j}>) and from
 * W matrix (w_i) obtained by DistributedCache.
 */
public class VectorByItemHDFSMapper
        extends AbstractJoinVectorMapper<IntPairWritable, FloatWritable> {

    @Override
    protected void map(final IntPairWritable key, final FloatWritable column, final Context context)
            throws IOException, InterruptedException {

        final float score = column.get();

        if (score <= 0) {
            return;
        }

        int user = key.getFirst();
        int item = key.getSecond();

        if (existsMapping()) {
            if ((user = getNewUserId(user)) != -1 && (item = getNewItemId(item)) != -1) {
                context.write(new IntWritable(user),
                        new VectorWritable(getCache(item).times(score)));
            }
        } else {
            context.write(new IntWritable(user), new VectorWritable(getCache(item).times(score)));
        }

    }

}
