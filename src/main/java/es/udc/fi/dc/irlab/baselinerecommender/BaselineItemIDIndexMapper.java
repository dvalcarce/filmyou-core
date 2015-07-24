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

package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

/**
 * Initial stage of the baseline recommendation algorithm. Modified because of
 * Cassandra integration.
 */
public final class BaselineItemIDIndexMapper extends
        Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, VarIntWritable, VarLongWritable> {

    @Override
    protected void map(final Map<String, ByteBuffer> keys, final Map<String, ByteBuffer> columns,
            final Context context) throws IOException, InterruptedException {

        final long itemID = keys.get("item").getInt();
        final int index = TasteHadoopUtils.idToIndex(itemID);
        context.write(new VarIntWritable(index), new VarLongWritable(itemID));
    }

}
