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

package es.udc.fi.dc.irlab.nmf.common;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Load matrix from DistributedCache in the setup phase.
 * 
 */
public abstract class AbstractDistributedMatrixMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	protected Matrix C;

	/**
	 * Load matrix from DistributedCache.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length < 1) {
			throw new FileNotFoundException();
		}

		Reader[] readers = HadoopUtils.getLocalSequenceReaders(paths[0], conf);

		NullWritable key = NullWritable.get();
		MatrixWritable val = new MatrixWritable();

		for (Reader reader : readers) {
			if (reader.next(key, val)) {
				C = val.get();
			} else {
				throw new FileNotFoundException(getClass()
						+ ": Invalid C file.");
			}
		}

	}

}
