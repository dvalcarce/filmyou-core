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

package es.udc.fi.dc.irlab.ppc.hcomputation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
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
import org.apache.hadoop.util.Tool;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.ppc.util.IntPairKeyPartitioner;

public class ComputeHJob extends AbstractJob implements Tool {

    private final Path H;
    private final Path W;
    private Path out1;
    private Path X;
    private Path C;
    private Path Y;
    private Path H2;

    /**
     * ComputeHJob constructor.
     * 
     * @param H
     *            Path to the H matrix
     * @param W
     *            Path to the W matrix
     */
    public ComputeHJob(Path H, Path W) {
	this.H = H;
	this.W = W;
    }

    /**
     * Load default command line arguments.
     */
    protected void loadDefaultSetup() {
	addOption("numberOfUsers", "n", "Number of users", true);
	addOption("numberOfItems", "m", "Number of movies", true);
	addOption("numberOfClusters", "k", "Number of PPC clusters", true);
	addOption("numberOfIterations", "i", "Number of PPC iterations", "1");
	addOption("directory", "d", "Working directory", "ppc");
	addOption("cassandraPort", "port", "Cassandra TCP port", "9160");
	addOption("cassandraHost", "host", "Cassandra host IP", "127.0.0.1");
	addOption("cassandraKeyspace", "keyspace", "Cassandra keyspace name",
		true);
	addOption("cassandraTable", "table", "Cassandra Column Family name",
		true);
	addOption("cassandraPartitioner", "partitioner",
		"Cassandra Partitioner",
		"org.apache.cassandra.dht.Murmur3Partitioner");
    }

    /**
     * Run all chained map-reduce jobs in order to compute H matrix.
     */
    @Override
    public int run(String[] args) throws Exception {
	loadDefaultSetup();

	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    throw new IllegalArgumentException("Invalid arguments");
	}

	String directory = getOption("directory");
	this.out1 = new Path(directory + "/hout1");
	this.X = new Path(directory + "/X");
	this.C = new Path(directory + "/C");
	this.Y = new Path(directory + "/Y");
	this.H2 = new Path(directory + "/H2");

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
     *            initial H path
     * @param outputPath
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob1(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job H1");
	job.setJarByClass(ComputeHJob.class);

	MultipleInputs.addInputPath(job, new Path("unused"),
		CqlPagingInputFormat.class, H1aMapper.class);
	MultipleInputs.addInputPath(job, inputPath,
		SequenceFileInputFormat.class, H1bMapper.class);

	job.setReducerClass(H1Reducer.class);

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
     * Launch the second job for H computation.
     * 
     * @param inputPath
     *            output of the first job
     * @param outputPath
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob2(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job H2");
	job.setJarByClass(ComputeHJob.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(H2Mapper.class);
	job.setReducerClass(H2Reducer.class);

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
     *            output of the first job
     * @param outputPath
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob3(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job H3");
	job.setJarByClass(ComputeHJob.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(H3Mapper.class);
	job.setReducerClass(H3Reducer.class);

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
     *            output of the first job
     * @param outputPath
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob4(Path inputPath, Path outputPath, Path cachePath)
	    throws IOException, ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job H4");
	job.setJarByClass(ComputeHJob.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(H4Mapper.class);
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

	Job job = new Job(getConf(), "Job H5");
	job.setJarByClass(ComputeHJob.class);

	MultipleInputs.addInputPath(job, inputPathH,
		SequenceFileInputFormat.class, H5HMapper.class);
	MultipleInputs.addInputPath(job, inputPathX,
		SequenceFileInputFormat.class, H5XMapper.class);
	MultipleInputs.addInputPath(job, inputPathY,
		SequenceFileInputFormat.class, H5YMapper.class);

	job.setReducerClass(H5Reducer.class);

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
