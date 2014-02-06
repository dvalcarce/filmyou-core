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

package es.udc.fi.dc.irlab.rm;

import java.io.IOException;

import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HDFSUtils;

/**
 * Relevance Model 2 job.
 * 
 */
public class RM2Job extends AbstractJob {

    private String directory;

    private Path userSum;
    private Path movieSum;
    private Path totalSum;

    /**
     * Run all chained map-reduce jobs in order to compute RM2.
     * 
     * @return int
     */
    @Override
    public int run(String[] args) throws Exception {
	directory = getConf().get("directory") + "/rm2";

	HDFSUtils.removeData(getConf(), directory);

	userSum = new Path(directory + "/userSum");
	movieSum = new Path(directory + "/movieSum");
	totalSum = new Path(directory + "/totalSum");

	runJob1(userSum);
	runJob2(movieSum);
	runJob3(movieSum, totalSum);

	return 0;
    }

    /**
     * Calculates the sum of the ratings of each user.
     * 
     * @param outputPath
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob1(Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job RM2-1");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(CqlPagingInputFormat.class);

	job.setMapperClass(SimpleScoreByUserMapper.class);
	job.setCombinerClass(SumReducer.class);
	job.setReducerClass(SumReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(FloatWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(FloatWritable.class);

	Configuration jobConf = job.getConfiguration();
	CassandraSetup.updateConf(getConf(), jobConf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Calculates the sum of the ratings of each movie.
     * 
     * @param outputPath
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob2(Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job RM2-2");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(CqlPagingInputFormat.class);

	job.setMapperClass(SimpleScoreByMovieMapper.class);
	job.setCombinerClass(SumReducer.class);
	job.setReducerClass(SumReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(FloatWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(FloatWritable.class);

	Configuration jobConf = job.getConfiguration();
	CassandraSetup.updateConf(getConf(), jobConf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Calculates the sum of the ratings of the collection.
     * 
     * @param inputPath
     * @param outputPath
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob3(Path inputPath, Path outputPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "Job RM2-3");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(NullMapper.class);
	job.setCombinerClass(SumReducer.class);
	job.setReducerClass(SumReducer.class);

	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(FloatWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(FloatWritable.class);

	Configuration jobConf = job.getConfiguration();
	CassandraSetup.updateConf(getConf(), jobConf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

}
