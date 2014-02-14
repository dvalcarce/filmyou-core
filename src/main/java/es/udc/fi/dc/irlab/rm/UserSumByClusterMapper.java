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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.util.MapFileOutputFormat;

/**
 * Emit &lt;(k, 1), (j, sum_i A_{i,j})> from &lt;j, sum_i A_{i,j}> where j is a
 * user from the cluster k.
 */
public class UserSumByClusterMapper extends
	AbstractByClusterMapper<IntWritable, DoubleWritable> {

    @Override
    protected void map(IntWritable key, DoubleWritable column, Context context)
	    throws IOException, InterruptedException {

	Configuration conf = context.getConfiguration();

	Reader[] readers = MapFileOutputFormat.getReaders(clustering, conf);
	Partitioner<IntWritable, IntWritable> partitioner = new HashPartitioner<IntWritable, IntWritable>();

	IntWritable cluster = new IntWritable();

	if (MapFileOutputFormat.getEntry(readers, partitioner, key, cluster) == null) {
	    throw new RuntimeException("User " + key + " not found");
	}

	context.write(new IntPairWritable(cluster.get(), 1),
		new IntDoubleOrPrefWritable(key.get(), column.get()));

    }
}
