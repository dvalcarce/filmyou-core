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

package es.udc.fi.dc.irlab.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Abstract mapper for reading clustering data as a SequenceFile in HDFS.
 * 
 * @param <A>
 *            Mapper input key class
 * @param <B>
 *            Mapper input value class
 */
public abstract class AbstractByClusterAndCountMapper<A, B, C, D> extends
		AbstractByClusterMapper<A, B, C, D> {

	private int[] clusterSizes;
	private Configuration conf;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		conf = context.getConfiguration();

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length < 2) {
			throw new FileNotFoundException();
		}

		super.setup(context);

		int nubmerOfClusters = conf.getInt(
				RMRecommenderDriver.numberOfClusters, -1);
		final int numberOfSubClusters = conf.getInt(
				RMRecommenderDriver.numberOfSubClusters, -1);
		if (numberOfSubClusters > 0) {
			nubmerOfClusters *= numberOfSubClusters;
		}

		clusterSizes = new int[nubmerOfClusters];

		SequenceFile.Reader[] readers = HadoopUtils.getLocalSequenceReaders(
				paths[1], conf);

		IntWritable key = new IntWritable();
		IntWritable val = new IntWritable();

		// Read cluster count
		for (SequenceFile.Reader reader : readers) {
			while (reader.next(key, val)) {
				clusterSizes[key.get()] = val.get();
			}
		}

	}

	/**
	 * Get the cluster split for the given user.
	 * 
	 * @param user
	 *            user id
	 * @return cluster split
	 */
	protected String[] getSplits(int user) {
		int cluster = getCluster(user);
		int size = clusterSizes[cluster];
		int clusterSplit = conf.getInt(RMRecommenderDriver.clusterSplit, -1);
		if (clusterSplit < 0) {
			return new String[] { String.valueOf(cluster) };
		}
		List<String> splits = new ArrayList<String>();
		int numberOfSplits = (int) Math.round(Math.ceil(size
				/ (double) clusterSplit));
		for (int i = 0; i < numberOfSplits; i++) {
			splits.add(String.format(Locale.ENGLISH, "%d-%d-%d", cluster, i,
					numberOfSplits));
		}

		return splits.toArray(new String[0]);
	}

}
