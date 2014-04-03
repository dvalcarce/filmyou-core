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

import java.io.IOException;

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
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.nmf.common.CrossProductMapper;
import es.udc.fi.dc.irlab.nmf.common.SumMatrixReducer;
import es.udc.fi.dc.irlab.nmf.common.SumVectorReducer;
import es.udc.fi.dc.irlab.nmf.common.Vector0Mapper;
import es.udc.fi.dc.irlab.nmf.common.Vector1Mapper;
import es.udc.fi.dc.irlab.nmf.common.Vector2Mapper;
import es.udc.fi.dc.irlab.nmf.util.IntPairKeyPartitioner;
import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HDFSUtils;

public class ComputeHJob extends MatrixComputationJob {

    protected static String prefix = "NMF";

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

	iteration = conf.getInt("iteration", -1);
	directory = conf.get("directory") + "/hcomputation";

	HDFSUtils.removeData(conf, directory);
	HDFSUtils.createFolder(conf, directory);

	this.out1 = new Path(directory + "/hout1");
	this.X = new Path(directory + "/X");
	this.C = new Path(directory + "/C");
	this.Y = new Path(directory + "/Y");

	runJob1(W, out1);
	runJob2(out1, X);
	runJob3(W, C);
	runJob4(H, Y, C);
	runJob5(H, X, Y, H2);

	return 0;
    }

    /**
     * Launch the first job for H computation.
     * 
     * @param inputPath
     *            initial W path
     * @param outputPath
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob1(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), prefix + "H1-it" + iteration);
	job.setJarByClass(this.getClass());

	MultipleInputs.addInputPath(job, new Path("unused"),
		CqlPagingInputFormat.class, ScoreByMovieMapper.class);

	job.setNumReduceTasks(0);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	Configuration conf = job.getConfiguration();

	Path distributedPath;
	try {
	    distributedPath = HDFSUtils.mergeFile(conf, inputPath, directory,
		    "W-merged");
	} catch (IllegalArgumentException e) {
	    distributedPath = inputPath;
	}
	DistributedCache.addCacheFile(distributedPath.toUri(), conf);

	CassandraSetup.updateConfForInput(getConf(), conf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Launch the second job for H computation.
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

	Job job = new Job(getConf(), prefix + "H2-it" + iteration);
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(Mapper.class);
	job.setReducerClass(SumVectorReducer.class);

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
     * Launch the third job for H computation.
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

	Job job = new Job(getConf(), prefix + "H3-it" + iteration);
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(CrossProductMapper.class);
	job.setReducerClass(SumMatrixReducer.class);

	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(MatrixWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setNumReduceTasks(1);

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
     * @param cPath
     *            C path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob4(Path inputPath, Path outputPath, Path cPath)
	    throws IOException, ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), prefix + "H4-it" + iteration);
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(CHMapper.class);
	job.setNumReduceTasks(0);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	Configuration conf = job.getConfiguration();
	Path mergedPath = HDFSUtils.mergeFile(conf, cPath, directory,
		"C-merged");
	DistributedCache.addCacheFile(mergedPath.toUri(), conf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Launch the fifth job for H computation.
     * 
     * @param inputPathH
     *            initial H path
     * @param inputPathX
     *            input X
     * @param inputPathY
     *            input Y
     * @param outputPath
     *            output H path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob5(Path inputPathH, Path inputPathX, Path inputPathY,
	    Path outputPath) throws IOException, ClassNotFoundException,
	    InterruptedException {

	Job job = new Job(getConf(), prefix + "H5-it" + iteration);
	job.setJarByClass(this.getClass());

	MultipleInputs.addInputPath(job, inputPathH,
		SequenceFileInputFormat.class, Vector0Mapper.class);
	MultipleInputs.addInputPath(job, inputPathX,
		SequenceFileInputFormat.class, Vector1Mapper.class);
	MultipleInputs.addInputPath(job, inputPathY,
		SequenceFileInputFormat.class, Vector2Mapper.class);

	job.setReducerClass(HComputationReducer.class);

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
