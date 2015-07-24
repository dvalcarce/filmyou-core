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
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

import es.udc.fi.dc.irlab.common.AbstractByClusterMapper;

/**
 * Emit &lt;k, i> from Cassandra ratings ({A_{i,j}}) where k is the cluster of
 * user j.
 */
public class ItemByClusterCassandraMapper extends
        AbstractByClusterMapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, IntWritable, IntWritable> {

    @Override
    protected void map(final Map<String, ByteBuffer> keys, final Map<String, ByteBuffer> columns,
            final Context context) throws IOException, InterruptedException {

        final float score = columns.get("score").getFloat();

        if (score > 0) {
            context.write(new IntWritable(getCluster(keys.get("user").getInt())),
                    new IntWritable(keys.get("item").getInt()));
        }

    }

}
