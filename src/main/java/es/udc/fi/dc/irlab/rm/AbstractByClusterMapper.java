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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.util.MapFileOutputFormat;

/**
 * Abstract mapper for reading clustering data as a MapFile in HDFS.
 * 
 * @param <A>
 *            Mapper input key class
 * @param <B>
 *            Mapper input value class
 */
public abstract class AbstractByClusterMapper<A, B> extends
	Mapper<A, B, IntPairWritable, IntDoubleOrPrefWritable> {

    protected Path clustering;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	clustering = new Path(conf.get("clustering"));
    }

    protected int getCluster(IntWritable key, Context context)
	    throws IOException {
	Configuration conf = context.getConfiguration();

	Reader[] readers = MapFileOutputFormat.getReaders(clustering, conf);
	Partitioner<IntWritable, IntWritable> partitioner = new HashPartitioner<IntWritable, IntWritable>();

	IntWritable cluster = new IntWritable();

	if (MapFileOutputFormat.getEntry(readers, partitioner, key, cluster) == null) {
	    throw new RuntimeException("Key " + key.get() + " not found");
	}

	return cluster.get();
    }

}
