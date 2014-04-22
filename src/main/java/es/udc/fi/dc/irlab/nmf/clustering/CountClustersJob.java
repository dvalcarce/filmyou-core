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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Count the number of elements in each cluster.
 * 
 */
public class CountClustersJob extends AbstractJob {

	@Override
	public int run(String[] args) throws Exception {
		Path clustering;
		Path clusteringCount;
		Configuration conf = getConf();

		/* Prepare paths */
		String directory = conf.get(RMRecommenderDriver.directory);
		clustering = new Path(directory + File.separator
				+ conf.get(RMRecommenderDriver.clustering));
		clusteringCount = new Path(directory + File.separator
				+ RMRecommenderDriver.clusteringCountPath);
		HadoopUtils.removeData(conf, clusteringCount.toString());

		/* Prepare job */
		Job job = new Job(HadoopUtils.sanitizeConf(getConf()), this.getClass()
				.getSimpleName());
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, clustering);

		job.setMapperClass(InverseMapper.class);
		job.setReducerClass(CountReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, clusteringCount);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}
		return 0;
	}

}
