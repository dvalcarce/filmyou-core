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

package es.udc.fi.dc.irlab.nmf.common;

import es.udc.fi.dc.irlab.util.HadoopUtils;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * 
 * 
 * @param <A>
 *            input key
 * @param <B>
 *            input value
 */
public abstract class AbstractJoinVectorMapper<A, B> extends
		MappingsMapper<A, B, IntWritable, VectorWritable> {

	private TIntObjectMap<Vector> cache = new TIntObjectHashMap<Vector>();

	/**
	 * Build HashMap with DistributedCache data.
	 * 
	 * @param context
	 *            Context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		super.setup(context);

		Configuration conf = context.getConfiguration();

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length < 1) {
			throw new FileNotFoundException();
		}

		Reader[] readers = HadoopUtils.getLocalSequenceReaders(paths[0], conf);

		IntWritable key = new IntWritable();
		VectorWritable val = new VectorWritable();

		for (Reader reader : readers) {
			while (reader.next(key, val)) {
				cache.put(key.get(), val.get());
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
		return cache.get(key);
	}

}
