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
import java.nio.ByteBuffer;
import java.util.Map;

import es.udc.fi.dc.irlab.common.AbstractByClusterAndCountMapper;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.StringIntPairWritable;

/**
 * Emit &lt;(k, 1), (i, j, A_{i,j})> from Cassandra ratings ({A_{i,j}}) where j
 * is a user from the cluster k.
 */
public class ScoreByClusterCassandraMapper extends
        AbstractByClusterAndCountMapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, StringIntPairWritable, IntDoubleOrPrefWritable> {

    @Override
    protected void map(final Map<String, ByteBuffer> keys, final Map<String, ByteBuffer> columns,
            final Context context) throws IOException, InterruptedException {

        final float score = columns.get("score").getFloat();

        if (score > 0) {
            final int user = keys.get("user").getInt();
            final int item = keys.get("item").getInt();
            for (final String split : getSplits(user)) {
                context.write(new StringIntPairWritable(split, 1),
                        new IntDoubleOrPrefWritable(user, item, score));
            }

        }

    }

}
