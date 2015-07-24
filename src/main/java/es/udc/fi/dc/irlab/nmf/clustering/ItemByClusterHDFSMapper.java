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

package es.udc.fi.dc.irlab.nmf.clustering;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.common.AbstractByClusterMapper;

/**
 * Emit &lt;k, i> from HDFS ratings (<(i, j), A_{i,j}>) where k is the cluster
 * of user j.
 */
public class ItemByClusterHDFSMapper
        extends AbstractByClusterMapper<IntPairWritable, FloatWritable, IntWritable, IntWritable> {

    @Override
    protected void map(final IntPairWritable key, final FloatWritable column, final Context context)
            throws IOException, InterruptedException {

        final float score = column.get();

        if (score > 0) {
            context.write(new IntWritable(getCluster(key.getFirst())),
                    new IntWritable(key.getSecond()));
        }

    }

}
