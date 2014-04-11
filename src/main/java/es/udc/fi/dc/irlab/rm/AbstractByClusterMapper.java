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

import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.IntPairWritable;

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

    private Path[] paths;
    private TIntIntMap map = new TIntIntHashMap();

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {

	Configuration conf = context.getConfiguration();

	paths = DistributedCache.getLocalCacheFiles(conf);

	if (paths == null || paths.length != 3) {
	    throw new FileNotFoundException();
	}

	try (SequenceFile.Reader reader = new SequenceFile.Reader(
		FileSystem.getLocal(conf), paths[0], conf)) {
	    IntWritable key = new IntWritable();
	    IntWritable val = new IntWritable();

	    while (reader.next(key, val)) {
		map.put(key.get(), val.get());
	    }
	}

    }

    /**
     * Get the cluster for the given user.
     * 
     * @param user
     *            user id
     * @return cluster
     */
    protected int getCluster(int user) {
	return map.get(user);
    }

}
