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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.spectral.common.IntDoublePairWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.IntPairWritable;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.ConsistencyLevel;

@SuppressWarnings("unused")
public class PPCJob extends AbstractJob {

    public static class KeyPartitioner extends
	    Partitioner<IntPairWritable, IntDoublePairWritable> {

	@Override
	public int getPartition(IntPairWritable key,
		IntDoublePairWritable value, int numPartitions) {
	    return (new Integer(key.getFirst()).hashCode() & Integer.MAX_VALUE)
		    % numPartitions;
	}

    }

    /**
     * Main routine. Launch PCCJob job.
     * 
     * @param args
     *            command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new PPCJob(), args);
    }

    /**
     * Load default command line arguments.
     */
    protected void loadDefaultSetup() {
	// addOption("name", "short", "description", "default");
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

	if (runJob1() < 0) {
	    return -2;
	}

	return 0;
    }

    private void createFiles() throws IOException {
	String uri1 = "a.txt";
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(uri1), conf);
	Path path = new Path(uri1);

	LongWritable key = new LongWritable();
	Text value = new Text();
	SequenceFile.Writer writer = null;
	try {
	    writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
		    value.getClass());

	    writer.append(new LongWritable(1), new Text("hola"));
	    writer.append(new LongWritable(2), new Text("adios"));
	} finally {
	    IOUtils.closeStream(writer);
	}

	String uri2 = "w.txt";
	Configuration conf2 = new Configuration();
	FileSystem fs2 = FileSystem.get(URI.create(uri2), conf2);
	Path path2 = new Path(uri2);

	LongWritable key2 = new LongWritable();
	Text value2 = new Text();
	SequenceFile.Writer writer2 = null;
	try {
	    writer2 = SequenceFile.createWriter(fs2, conf2, path2,
		    key2.getClass(), value2.getClass());

	    writer2.append(new LongWritable(1), new Text("pepe"));
	    writer2.append(new LongWritable(2), new Text("fulanito"));
	} finally {
	    IOUtils.closeStream(writer2);
	}

    }

    protected int runJob1() throws IOException, ClassNotFoundException,
	    InterruptedException {
	Job job1 = new Job(getConf(), "Job #1");
	job1.setJarByClass(PPCJob.class);

	Path inputFile = new Path("w.txt");
	Path outputDir = new Path("out");

	MultipleInputs.addInputPath(job1, new Path("unused"),
		CqlPagingInputFormat.class, Map1a.class);
	MultipleInputs.addInputPath(job1, inputFile,
		KeyValueTextInputFormat.class, Map1b.class);

	job1.setReducerClass(Reducer1.class);
	job1.setMapOutputKeyClass(IntPairWritable.class);
	job1.setMapOutputValueClass(IntDoublePairWritable.class);
	job1.setOutputKeyClass(LongWritable.class);
	job1.setOutputValueClass(Text.class);

	job1.setPartitionerClass(KeyPartitioner.class);
	job1.setSortComparatorClass(IntPairWritable.Comparator.class);
	job1.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

	TextOutputFormat.setOutputPath(job1, outputDir);

	Configuration conf = job1.getConfiguration();
	// Cassandra settings
	String port = "9160";
	String host = "127.0.0.1";
	String keyspace = "recommender";
	String table = "ratings";
	ConfigHelper.setInputRpcPort(conf, port);
	ConfigHelper.setInputInitialAddress(conf, host);
	ConfigHelper.setInputPartitioner(conf,
		"org.apache.cassandra.dht.Murmur3Partitioner");
	ConfigHelper.setInputColumnFamily(conf, keyspace, table, true);
	// FIXME
	ConfigHelper.setReadConsistencyLevel(conf, "ONE");
	ConfigHelper.setInputSplitSize(conf, 262144);
	ConfigHelper.setRangeBatchSize(conf, 262144);
	boolean succeeded = job1.waitForCompletion(true);
	if (!succeeded) {
	    return -1;
	}
	return 0;
    }
}
