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

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.ppc.util.IntPairKeyPartitioner;

public class ComputeHJob extends AbstractJob implements Tool {

    private final Path H;
    private Path out1;
    private Path out2;

    /**
     * ComputeHJob constructor.
     * 
     * @param H
     *            Path to the H matrix
     */
    public ComputeHJob(Path H) {
	this.H = H;
    }

    /**
     * Run all chained map-reduce jobs in order to compute H matrix.
     */
    @Override
    public int run(String[] args) throws Exception {
	parseArguments(args, true, true);

	String directory = getOption("directory");
	this.out1 = new Path(directory + "/out1");
	this.out2 = new Path(directory + "/out2");

	runJob1(H, out1);
	runJob2(out1, out2);

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

	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

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

	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);

	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(VectorWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

}
