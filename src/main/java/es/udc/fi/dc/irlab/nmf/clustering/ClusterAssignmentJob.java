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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Assign each user to a cluster after NMF/PPC execution.
 * 
 */
public class ClusterAssignmentJob extends AbstractJob {

	private final boolean doSubClustering;

	public ClusterAssignmentJob(boolean doSubClustering) {
		this.doSubClustering = doSubClustering;
	}

	@Override
	public int run(String[] args) throws ClassNotFoundException, IOException,
			InterruptedException {

		Path clustering;
		Configuration conf = getConf();

		/* Prepare paths */
		String directory = conf.get(RMRecommenderDriver.directory);
		clustering = new Path(directory + File.separator
				+ conf.get(RMRecommenderDriver.clustering));

		HadoopUtils.removeData(conf, clustering.toString());

		if (doSubClustering) {
			Path H_join = new Path(directory + File.separator
					+ RMRecommenderDriver.joinPath);
			findSubClusters(H_join, clustering);
		} else {
			Path H = new Path(conf.get(RMRecommenderDriver.H));
			findClusters(H, clustering);
		}

		return 0;

	}

	public void findClusters(Path inputPath, Path outputPath)
			throws ClassNotFoundException, IOException, InterruptedException {

		Job job = new Job(HadoopUtils.sanitizeConf(getConf()), "Find Clusters");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, inputPath);

		job.setMapperClass(FindClusterMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}
	}

	public void findSubClusters(Path inputPaths, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = getConf();
		Job job = new Job(HadoopUtils.sanitizeConf(conf), "Find SubClusters");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		int numberOfClusters = conf.getInt(
				RMRecommenderDriver.numberOfClusters, -1);
		Path path;
		for (int i = 0; i < numberOfClusters; i++) {
			path = new Path(inputPaths.toString() + File.separator + "cluster"
					+ i);
			SequenceFileInputFormat.addInputPath(job, path);
		}

		job.setMapperClass(FindSubClusterMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

}
