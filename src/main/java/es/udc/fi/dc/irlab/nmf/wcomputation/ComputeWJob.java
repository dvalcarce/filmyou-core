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

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.nmf.util.IntPairKeyPartitioner;

public class ComputeWJob extends MatrixComputationJob {

    private String directory;

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
    public ComputeWJob(Path H, Path W, Path H2, Path W2) {
	super(H, W, H2, W2);
    }

    /**
     * Run all chained map-reduce jobs in order to compute H matrix.
     */
    @Override
    public int run(String[] args) throws Exception {
	parseInput(args);

	directory = getOption("directory") + "/wcomputation";

	cleanPreviousData(directory);

	this.out1 = new Path(directory + "/wout1");
	this.X = new Path(directory + "/X");
	this.C = new Path(directory + "/C");
	this.Y = new Path(directory + "/Y");

	runJob1(H, out1);
	runJob2(out1, X);
	runJob3(H, C);
	runJob4(W, Y, C);
	runJob5(W, X, Y, W2);

	return 0;
    }

    /**
     * Launch the first job for W computation.
     * 
     * @param inputPath
     *            initial H path
     * @param outputPath
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob1(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job W1");
	job.setJarByClass(ComputeWJob.class);

	MultipleInputs.addInputPath(job, new Path("unused"),
		CqlPagingInputFormat.class, W1aMapper.class);
	MultipleInputs.addInputPath(job, inputPath,
		SequenceFileInputFormat.class, W1bMapper.class);

	job.setReducerClass(W1Reducer.class);

	job.setMapOutputKeyClass(IntPairWritable.class);
	job.setMapOutputValueClass(VectorOrPrefWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	job.setPartitionerClass(IntPairKeyPartitioner.class);
	job.setSortComparatorClass(IntPairWritable.Comparator.class);
	job.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

	// Cassandra settings
	Configuration conf = job.getConfiguration();

	ConfigHelper.setInputRpcPort(conf, getOption("cassandraPort"));
	ConfigHelper.setInputInitialAddress(conf, getOption("cassandraHost"));
	ConfigHelper.setInputPartitioner(conf,
		getOption("cassandraPartitioner"));
	ConfigHelper.setInputColumnFamily(conf, getOption("cassandraKeyspace"),
		getOption("cassandraTable"), true);
	ConfigHelper.setReadConsistencyLevel(conf, "ONE");

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Launch the second job for W computation.
     * 
     * @param inputPath
     *            output of the first job
     * @param outputPath
     *            X path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob2(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job W2");
	job.setJarByClass(ComputeWJob.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(Mapper.class);
	job.setReducerClass(W2Reducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Launch the third job for W computation.
     * 
     * @param inputPath
     *            W path
     * @param outputPath
     *            C path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob3(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job W3");
	job.setJarByClass(ComputeWJob.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(W3Mapper.class);
	job.setReducerClass(W3Reducer.class);

	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(MatrixWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(MatrixWritable.class);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Launch the fourth job for H computation.
     * 
     * @param inputPath
     *            H path
     * @param outputPath
     *            Y path
     * @param cachePath
     *            C path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob4(Path inputPath, Path outputPath, Path cachePath)
	    throws IOException, ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job W4");
	job.setJarByClass(ComputeWJob.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(W4Mapper.class);
	job.setNumReduceTasks(0);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	DistributedCache
		.addCacheFile(cachePath.toUri(), job.getConfiguration());

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Launch the fifth job for H computation.
     * 
     * @param inputPathW
     *            initial W path
     * @param inputPathX
     *            input X
     * @param inputPathY
     *            input Y
     * @param outputPath
     *            output W path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob5(Path inputPathH, Path inputPathX, Path inputPathY,
	    Path outputPath) throws IOException, ClassNotFoundException,
	    InterruptedException {

	Job job = new Job(getConf(), "Job W5");
	job.setJarByClass(ComputeWJob.class);

	MultipleInputs.addInputPath(job, inputPathH,
		SequenceFileInputFormat.class, W5WMapper.class);
	MultipleInputs.addInputPath(job, inputPathX,
		SequenceFileInputFormat.class, W5XMapper.class);
	MultipleInputs.addInputPath(job, inputPathY,
		SequenceFileInputFormat.class, W5YMapper.class);

	job.setReducerClass(W5Reducer.class);

	job.setMapOutputKeyClass(IntPairWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	job.setPartitionerClass(IntPairKeyPartitioner.class);
	job.setSortComparatorClass(IntPairWritable.Comparator.class);
	job.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

}
