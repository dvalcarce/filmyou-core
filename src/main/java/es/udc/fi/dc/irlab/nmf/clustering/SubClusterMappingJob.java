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

import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Make mappings for users and items.
 * 
 */
public class SubClusterMappingJob extends AbstractJob {

	public static final String output = "output";

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {

		Path inputPath;
		Path clustering;
		Path subClusteringUser;
		Path subClusteringItem;
		String directory;

		Configuration conf = getConf();

		/* Prepare paths */
		directory = conf.get(RMRecommenderDriver.directory);
		clustering = new Path(directory + File.separator
				+ conf.get(RMRecommenderDriver.clustering));
		subClusteringUser = new Path(directory + File.separator
				+ RMRecommenderDriver.subClusteringUserPath);
		subClusteringItem = new Path(directory + File.separator
				+ RMRecommenderDriver.subClusteringItemPath);
		inputPath = HadoopUtils.getInputPath(conf);

		HadoopUtils.removeData(conf, subClusteringUser.toString());
		HadoopUtils.removeData(conf, subClusteringItem.toString());

		mapUsers(clustering, subClusteringUser);
		mapItems(inputPath, subClusteringItem, clustering);

		return 0;
	}

	private void mapUsers(Path inputPath, Path outputPath) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = getConf();

		/* Prepare job */
		Job job = new Job(HadoopUtils.sanitizeConf(conf), "UserMappingJob");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, inputPath);

		job.setMapperClass(InverseMapper.class);
		job.setReducerClass(UserMappingReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		int numberOfClusters = conf.getInt(
				RMRecommenderDriver.numberOfClusters, 0);
		job.setNumReduceTasks(Math.min(numberOfClusters,
				job.getNumReduceTasks()));

		/* Configure multiple outputs */
		MultipleOutputs.addNamedOutput(job, output,
				SequenceFileOutputFormat.class, IntWritable.class,
				IntWritable.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

	private void mapItems(Path inputPath, Path outputPath, Path clustering)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = getConf();

		/* Prepare job */
		Job job = new Job(HadoopUtils.sanitizeConf(conf), "ItemMappingJob");
		job.setJarByClass(this.getClass());

		Configuration jobConf = job.getConfiguration();

		if (conf.getBoolean("useCassandra", true)) {
			job.setInputFormatClass(CqlPagingInputFormat.class);
			job.setMapperClass(ItemByClusterCassandraMapper.class);
			CassandraSetup.updateConfForInput(conf, jobConf);
		} else {
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(ItemByClusterHDFSMapper.class);
			SequenceFileInputFormat.addInputPath(job, inputPath);
		}

		job.setReducerClass(ItemMappingReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		int numberOfClusters = conf.getInt(
				RMRecommenderDriver.numberOfClusters, 0);
		job.setNumReduceTasks(Math.min(numberOfClusters,
				job.getNumReduceTasks()));

		/* Configure multiple outputs */
		MultipleOutputs.addNamedOutput(job, output,
				SequenceFileOutputFormat.class, IntWritable.class,
				IntWritable.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		DistributedCache.addCacheFile(clustering.toUri(), jobConf);
		jobConf.setInt(MatrixComputationJob.numberOfFiles, 1);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

}
