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

package es.udc.fi.dc.irlab.nmf.util;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit &lt;(i, 0), w_i> from H matrix (w_i).
 * 
 */

public abstract class AbstractJoinVectorMapper<A, B> extends
	Mapper<A, B, IntWritable, VectorWritable> {

    private Path[] paths;
    private TIntObjectMap<Vector> map = new TIntObjectHashMap<Vector>();

    /**
     * Build HashMap with DistributedCache data.
     */
    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {

	Configuration conf = context.getConfiguration();

	paths = DistributedCache.getLocalCacheFiles(conf);

	if (paths == null || paths.length < 1) {
	    throw new FileNotFoundException();
	}

	for (Path path : paths) {
	    try (SequenceFile.Reader reader = new SequenceFile.Reader(
		    FileSystem.getLocal(conf), path, conf)) {
		IntWritable key = new IntWritable();
		VectorWritable val = new VectorWritable();

		while (reader.next(key, val)) {
		    map.put(key.get(), val.get());
		}
	    }
	}
    }

    /**
     * Get cached Vector of the given key. Used for replication join.
     * 
     * @param key
     *            integer key
     * @return Vector
     */
    protected Vector getCache(int key) {
	return map.get(key);
    }

}
