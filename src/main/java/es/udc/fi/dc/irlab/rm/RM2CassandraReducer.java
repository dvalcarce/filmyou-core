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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Emit &lt;j, i, A_{i,j}> to Cassandra from &lt;k, {|k|} U {(j, sum_i A_{i,j})}
 * U {(i, j, A_{i,j})}>.
 */
public class RM2CassandraReducer
        extends AbstractRM2Reducer<Map<String, ByteBuffer>, List<ByteBuffer>> {

    /**
     * Write preference to Cassandra.
     *
     * @param context
     *            reduce context
     * @param userId
     *            user ID
     * @param itemId
     *            item ID
     * @param score
     *            predicted score
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void writePreference(final Context context, final int userId, final int itemId,
            final double score, final int cluster) throws IOException, InterruptedException {

        final Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
        keys.put("user", ByteBufferUtil.bytes(userId));
        keys.put("item", ByteBufferUtil.bytes(itemId));
        keys.put("relevance", ByteBufferUtil.bytes((float) score));

        final List<ByteBuffer> value = new LinkedList<ByteBuffer>();
        value.add(ByteBufferUtil.bytes(cluster));

        context.write(keys, value);

    }

}
