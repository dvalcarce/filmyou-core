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

package es.udc.fi.dc.irlab.nmf.wcomputation;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;

/**
 * Emit &lt;i, w_i · x_i / y_i> from &lt;i, {w_i, x_i, y_i}>.
 */
public class WComputationMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	private Path[] paths;
	private TIntObjectMap<Vector> mapX;
	private TIntObjectMap<Vector> mapY;

	/**
	 * Build HashMaps with DistributedCache data.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();

		paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length != 2) {
			throw new FileNotFoundException();
		}

		IntWritable key = new IntWritable();
		VectorWritable val = new VectorWritable();

		int numberOfItems = conf.getInt(RMRecommenderDriver.numberOfItems, -1);
		mapX = new TIntObjectHashMap<Vector>(numberOfItems);
		mapY = new TIntObjectHashMap<Vector>(numberOfItems);

		Reader[] readers = HadoopUtils.getLocalSequenceReaders(paths[0], conf);
		for (Reader reader : readers) {
			while (reader.next(key, val)) {
				mapX.put(key.get(), val.get());
			}
		}

		readers = HadoopUtils.getLocalSequenceReaders(paths[1], conf);
		for (Reader reader : readers) {
			while (reader.next(key, val)) {
				mapY.put(key.get(), val.get());
			}
		}

	}

	@Override
	protected void map(IntWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {

		int index = key.get();
		Vector vectorW = value.get();
		Vector vectorX = mapX.get(index);
		Vector vectorY = mapY.get(index);

		if (vectorX == null || vectorY == null) {
			throw new NoSuchElementException(String.format(
					"Item %d has not been rated by anybody", key.get()));
		}

		// Performs (X ./ Y)
		Vector vectorXY = vectorX.assign(vectorY, new DoubleDoubleFunction() {
			public double apply(double a, double b) {
				return a / b;
			}
		});

		context.write(key, new VectorWritable(vectorW.times(vectorXY)));

	}

}
