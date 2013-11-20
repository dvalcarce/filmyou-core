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

package es.udc.fi.dc.irlab.ppc;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class PPCDriver extends AbstractJob {

    private static int numberOfUsers;
    private static int numberOfItems;
    private static int numberOfClusters;

    public static class KeyPartitioner extends
	    Partitioner<IntPairWritable, Writable> {

	@Override
	public int getPartition(IntPairWritable key, Writable value,
		int numPartitions) {
	    return (new Integer(key.getFirst()).hashCode() & Integer.MAX_VALUE)
		    % numPartitions;
	}

    }

    /**
     * Load default command line arguments.
     */
    protected void loadDefaultSetup() {
	addOption("numberOfUsers", "m", "Number of users", true);
	addOption("numberOfItems", "n", "Number of movies", true);
	addOption("numberOfClusters", "k", "Number of clusters", true);
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
     * PPC algorithm.
     * 
     */
    @Override
    public int run(String[] args) throws Exception {
	loadDefaultSetup();

	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    return -1;
	}
	numberOfUsers = Integer.parseInt(getOption("numberOfUsers"));
	numberOfItems = Integer.parseInt(getOption("numberOfItems"));
	numberOfClusters = Integer.parseInt(getOption("numberOfClusters"));

	createInitialData("W", numberOfUsers, numberOfClusters);
	// createInitialData("H", numberOfItems, numberOfClusters);

	boolean converged = false;
	while (!converged) {
	    Path w = new Path("W");
	    Path out1 = new Path("out1");
	    Path out2 = new Path("out2");

	    if (runJob1(w, out1) < 0) {
		throw new RuntimeException("runJob1() has failed!");
	    }
	    if (runJob2(out1, out2) < 0) {
		throw new RuntimeException("runJob2() has failed!");
	    }
	    converged = true; // check_convergence();
	}
	return 0;
    }

    protected void createInitialData(String uri, int rows, int cols)
	    throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	Path path = new Path(uri);

	SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
		IntWritable.class, VectorWritable.class);

	Vector vector = new DenseVector(cols);
	Random randomGenerator = new Random();
	try {
	    for (int i = 0; i < rows; i++) {
		for (int j = 0; j < cols; j++) {
		    vector.setQuick(j, randomGenerator.nextDouble());
		}
		vector = vector.normalize(1);
		writer.append(new IntWritable(i), new VectorWritable(vector));
	    }
	} finally {
	    IOUtils.closeStream(writer);
	}

    }

    protected int runJob1(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {
	Job job1 = new Job(getConf(), "Job #1");
	job1.setJarByClass(PPCDriver.class);

	MultipleInputs.addInputPath(job1, new Path("unused"),
		CqlPagingInputFormat.class, Map1a.class);
	MultipleInputs.addInputPath(job1, inputPath,
		SequenceFileInputFormat.class, Map1b.class);

	job1.setReducerClass(Reducer1.class);

	job1.setMapOutputKeyClass(IntPairWritable.class);
	job1.setMapOutputValueClass(VectorOrPrefWritable.class);

	job1.setOutputKeyClass(LongWritable.class);
	job1.setOutputValueClass(VectorWritable.class);

	job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job1, outputPath);

	job1.setPartitionerClass(KeyPartitioner.class);
	job1.setSortComparatorClass(IntPairWritable.Comparator.class);
	job1.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

	// Cassandra settings
	Configuration conf = job1.getConfiguration();

	ConfigHelper.setInputRpcPort(conf, getOption("cassandraPort"));
	ConfigHelper.setInputInitialAddress(conf, getOption("cassandraHost"));
	ConfigHelper.setInputPartitioner(conf,
		getOption("cassandraPartitioner"));
	ConfigHelper.setInputColumnFamily(conf, getOption("cassandraKeyspace"),
		getOption("cassandraTable"), true);
	ConfigHelper.setReadConsistencyLevel(conf, "ONE");

	boolean succeeded = job1.waitForCompletion(true);
	if (!succeeded) {
	    return -1;
	}

	return 0;
    }

    protected int runJob2(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {
	Job job2 = new Job(getConf(), "Job #2");
	job2.setJarByClass(PPCDriver.class);

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job2, inputPath);

	job2.setMapperClass(Map2.class);
	job2.setReducerClass(Reducer2.class);

	job2.setMapOutputKeyClass(LongWritable.class);
	job2.setMapOutputValueClass(VectorWritable.class);

	job2.setOutputKeyClass(LongWritable.class);
	job2.setOutputValueClass(VectorWritable.class);

	job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job2, outputPath);

	boolean succeeded = job2.waitForCompletion(true);
	if (!succeeded) {
	    return -1;
	}

	return 0;
    }
}
