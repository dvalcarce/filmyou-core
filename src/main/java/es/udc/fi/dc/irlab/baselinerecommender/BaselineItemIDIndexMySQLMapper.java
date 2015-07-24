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

package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

/**
 * Initial stage of the baseline recommendation algorithm. Modified because of
 * MySQL integration.
 */
public final class BaselineItemIDIndexMySQLMapper
        extends Mapper<LongWritable, MySQLRecord, VarIntWritable, VarLongWritable> {

    @Override
    protected void map(final LongWritable key, final MySQLRecord value, final Context context)
            throws IOException, InterruptedException {

        final int index = TasteHadoopUtils.idToIndex(value.item);
        context.write(new VarIntWritable(index), new VarLongWritable(value.item));

    }

}
