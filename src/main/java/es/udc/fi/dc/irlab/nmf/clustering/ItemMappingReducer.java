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

package es.udc.fi.dc.irlab.nmf.clustering;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Generate new IDs for each cluster. This reducer use MultipleOutputs to write
 * cluster info in different files.
 * 
 * Input: &lt;clusterId, oldId>.
 * 
 * Outputs: &lt;oldId, newId> and &lt;cluster, numberOfObject>.
 */
public class ItemMappingReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private MultipleOutputs<IntWritable, IntWritable> mos;
	private TIntSet set;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		set = new TIntHashSet(conf.getInt(RMRecommenderDriver.numberOfItems, 0));
		mos = new MultipleOutputs<IntWritable, IntWritable>(context);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> items,
			Context context) throws IOException, InterruptedException {

		int cluster = key.get();
		int count = 0;
		int item;

		/* Write new user IDs */
		String mapping = RMRecommenderDriver.mappingPath + File.separator
				+ cluster;

		for (IntWritable i : items) {
			item = i.get();
			if (set.contains(item)) {
				continue;
			}
			set.add(item);
			mos.write(SubClusterMappingJob.output, i, new IntWritable(++count),
					mapping);
		}

		/* Write number of users in cluster */
		String clusteringCountOutput = RMRecommenderDriver.clusteringCountPath
				+ File.separator + "count";

		mos.write(SubClusterMappingJob.output, new IntWritable(cluster),
				new IntWritable(count), clusteringCountOutput);

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
