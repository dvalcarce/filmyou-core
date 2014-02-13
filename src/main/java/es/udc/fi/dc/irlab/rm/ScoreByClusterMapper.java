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

package es.udc.fi.dc.irlab.rm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.util.MapFileOutputFormat;

/**
 * Emit &lt;(k, 0), (i, j, A_{i,j})> from Cassandra ratings ({A_{i,j}}) where j
 * is a user from the cluster k.
 */
public class ScoreByClusterMapper
	extends
	Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, IntPairWritable, PreferenceWritable> {

    private Path clustering;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

	if (localFiles.length != 2) {
	    throw new FileNotFoundException(getClass()
		    + ": Missing distributed cache files.");
	}

	clustering = localFiles[0];
    }

    @Override
    protected void map(Map<String, ByteBuffer> keys,
	    Map<String, ByteBuffer> columns, Context context)
	    throws IOException, InterruptedException {

	int movie = keys.get("movie").getInt();
	int user = keys.get("user").getInt();
	float score = columns.get("score").getFloat();

	Configuration conf = context.getConfiguration();

	Reader[] readers = MapFileOutputFormat.getReaders(clustering, conf);
	Partitioner<IntWritable, IntWritable> partitioner = new HashPartitioner<IntWritable, IntWritable>();

	IntWritable key = new IntWritable(user);
	IntWritable cluster = new IntWritable();

	if (MapFileOutputFormat.getEntry(readers, partitioner, key, cluster) == null) {
	    throw new RuntimeException("User " + user + " not found");
	}

	context.write(new IntPairWritable(cluster.get(), 0),
		new PreferenceWritable(user, movie, score));

    }
}
