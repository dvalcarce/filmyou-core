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

package es.udc.fi.dc.irlab.nmf.hcomputation;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.nmf.common.CrossProductMapper;
import es.udc.fi.dc.irlab.nmf.common.MatrixSumReducer;
import es.udc.fi.dc.irlab.nmf.common.Vector0Mapper;
import es.udc.fi.dc.irlab.nmf.common.Vector1Mapper;
import es.udc.fi.dc.irlab.nmf.common.Vector2Mapper;
import es.udc.fi.dc.irlab.nmf.common.VectorSumReducer;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntPairKeyPartitioner;

public class ComputeHJob extends MatrixComputationJob {

	protected String prefix = "NMF";

	/**
	 * ComputeHJob constructor.
	 * 
	 * @param H
	 *            Path to the input H matrix
	 * @param W
	 *            Path to the input W matrix
	 * @param H2
	 *            Path to the output H matrix
	 * @param W2
	 *            Path to the output W matrix
	 */
	public ComputeHJob(Path H, Path W, Path H2, Path W2) {
		super(H, W, H2, W2);
	}

	/**
	 * Run all chained map-reduce jobs in order to compute H matrix.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		int c;
		if ((c = conf.getInt(RMRecommenderDriver.subClustering, -1)) >= 0) {
			cluster = "(" + c + ")";
		}
		inputPath = HadoopUtils.getInputPath(conf);
		iteration = conf.getInt(RMRecommenderDriver.iteration, -1);
		directory = conf.get(RMRecommenderDriver.directory) + File.separator
				+ "hcomputation";

		HadoopUtils.removeData(conf, directory);
		HadoopUtils.removeData(conf, H2.toString());
		HadoopUtils.createFolder(conf, directory);

		this.X = new Path(directory + File.separator + "X");
		this.C = new Path(directory + File.separator + "C");
		this.Y = new Path(directory + File.separator + "Y");

		runJob1(W, X);
		runJob2(W, C);
		runJob3(H, Y, C);
		runJob4(H, X, Y, H2);

		return 0;
	}

	/**
	 * Launch the first job for H computation.
	 * 
	 * @param wPath
	 *            initial W path
	 * @param xPath
	 *            X path
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	protected void runJob1(Path wPath, Path xPath) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = getConf();

		Job job = new Job(HadoopUtils.sanitizeConf(conf), prefix + "H1"
				+ cluster + "-it" + iteration);
		job.setJarByClass(this.getClass());

		Configuration jobConf = job.getConfiguration();

		if (conf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
			job.setInputFormatClass(CqlPagingInputFormat.class);
			job.setMapperClass(VectorByItemCassandraMapper.class);
			CassandraSetup.updateConfForInput(conf, jobConf);
		} else {
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(VectorByItemHDFSMapper.class);
			SequenceFileInputFormat.addInputPath(job, inputPath);
		}

		job.setCombinerClass(VectorSumReducer.class);
		job.setReducerClass(VectorSumReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, xPath);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		DistributedCache.addCacheFile(wPath.toUri(), jobConf);
		injectMappings(job, conf, true);
		jobConf.setInt(MatrixComputationJob.numberOfFiles, 3);

		jobConf.set(maxSplitSize, MB4);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

	/**
	 * Launch the second job for H computation.
	 * 
	 * @param wPath
	 *            W path
	 * @param cPath
	 *            C path
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	protected void runJob2(Path wPath, Path cPath) throws IOException,
			ClassNotFoundException, InterruptedException {

		Job job = new Job(HadoopUtils.sanitizeConf(getConf()), prefix + "H2"
				+ cluster + "-it" + iteration);
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, wPath);

		job.setMapperClass(CrossProductMapper.class);
		job.setCombinerClass(MatrixSumReducer.class);
		job.setReducerClass(MatrixSumReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(MatrixWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, cPath);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(MatrixWritable.class);

		Configuration jobConf = job.getConfiguration();
		jobConf.set(maxSplitSize, MB4);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

	/**
	 * Launch the third job for H computation.
	 * 
	 * @param hPath
	 *            H path
	 * @param yPath
	 *            Y path
	 * @param cPath
	 *            C path
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	protected void runJob3(Path hPath, Path yPath, Path cPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(HadoopUtils.sanitizeConf(getConf()), prefix + "H3"
				+ cluster + "-it" + iteration);
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, hPath);

		job.setMapperClass(CHMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, yPath);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		Configuration jobConf = job.getConfiguration();
		DistributedCache.addCacheFile(cPath.toUri(), jobConf);
		jobConf.setInt(MatrixComputationJob.numberOfFiles, 1);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

	/**
	 * Launch the fourth job for H computation.
	 * 
	 * @param hPath
	 *            initial H path
	 * @param xPath
	 *            input X
	 * @param yPath
	 *            input Y
	 * @param hOutputPath
	 *            output H path
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	protected void runJob4(Path hPath, Path xPath, Path yPath, Path hOutputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = getConf();

		Job job = new Job(HadoopUtils.sanitizeConf(conf), prefix + "H4"
				+ cluster + "-it" + iteration);
		job.setJarByClass(this.getClass());

		MultipleInputs.addInputPath(job, hPath, SequenceFileInputFormat.class,
				Vector0Mapper.class);
		MultipleInputs.addInputPath(job, xPath, SequenceFileInputFormat.class,
				Vector1Mapper.class);
		MultipleInputs.addInputPath(job, yPath, SequenceFileInputFormat.class,
				Vector2Mapper.class);

		job.setReducerClass(HComputationReducer.class);

		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, hOutputPath);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setPartitionerClass(IntPairKeyPartitioner.class);
		job.setSortComparatorClass(IntPairWritable.Comparator.class);
		job.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

		Configuration jobConf = job.getConfiguration();
		injectMappings(job, conf, false);
		jobConf.setInt(MatrixComputationJob.numberOfFiles, 1);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new RuntimeException(job.getJobName() + " failed!");
		}

	}

}
