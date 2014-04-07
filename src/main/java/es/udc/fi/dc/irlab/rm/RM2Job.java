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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.nmf.util.IntPairKeyPartitioner;
import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HDFSUtils;
import es.udc.fi.dc.irlab.util.MapFileOutputFormat;

/**
 * Relevance Model 2 job.
 * 
 */
public class RM2Job extends AbstractJob {

    public static final String totalSumName = "rm.totalSum";
    public static final String lambdaName = "lambda";

    private String directory;

    /**
     * Run all chained map-reduce jobs in order to compute RM2.
     * 
     * @return int
     */
    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	inputPath = new Path(conf.get(HDFSUtils.inputPathName));
	outputPath = new Path(conf.get(HDFSUtils.outputPathName));
	String baseDirectory = conf.get("directory");
	directory = baseDirectory + "/rm2";

	HDFSUtils.removeData(conf, directory);

	Path userSum = new Path(directory + "/userSum");
	Path itemSum = new Path(directory + "/movieSum");
	Path totalSum = new Path(directory + "/totalSum");
	Path itemColl = new Path(directory + "/itemColl");
	Path clustering = new Path(baseDirectory + "/" + conf.get("clustering"));
	Path clusteringCount = new Path(baseDirectory + "/"
		+ conf.get("clusteringCount"));

	runUserSum(userSum);
	runMovieSum(itemSum);
	runTotalSum(itemSum, totalSum);

	double sum = HDFSUtils.getDoubleFromSequenceFile(conf, totalSum,
		directory);

	runItemProbInCollection(itemSum, sum, itemColl);

	runItemRecommendation(userSum, clusteringCount, clustering, itemColl);

	return 0;
    }

    /**
     * Calculates the sum of the ratings of each user.
     * 
     * @param userSum
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runUserSum(Path userSum) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "RM2-1");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(CqlPagingInputFormat.class);

	job.setMapperClass(SimpleScoreByUserMapper.class);
	job.setCombinerClass(DoubleSumReducer.class);
	job.setReducerClass(DoubleSumReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, userSum);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(DoubleWritable.class);

	Configuration jobConf = job.getConfiguration();
	CassandraSetup.updateConfForInput(getConf(), jobConf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Calculates the sum of the ratings of each item.
     * 
     * @param itemSum
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runMovieSum(Path itemSum) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "RM2-2");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(CqlPagingInputFormat.class);

	job.setMapperClass(SimpleScoreByMovieMapper.class);
	job.setCombinerClass(DoubleSumReducer.class);
	job.setReducerClass(DoubleSumReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, itemSum);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(DoubleWritable.class);

	Configuration jobConf = job.getConfiguration();
	CassandraSetup.updateConfForInput(getConf(), jobConf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Calculates the sum of the ratings of the collection.
     * 
     * @param itemSum
     * @param totalSum
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runTotalSum(Path itemSum, Path totalSum) throws IOException,
	    ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "RM2-3");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, itemSum);

	job.setMapperClass(NullMapper.class);
	job.setCombinerClass(DoubleSumReducer.class);
	job.setReducerClass(DoubleSumReducer.class);

	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);

	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, totalSum);

	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(DoubleWritable.class);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Calculates the probability of a item in the collection.
     * 
     * @param itemSum
     *            sum of the ratings of each movie
     * @param totalSum
     *            global sum of the ratings
     * @param itemColl
     *            result
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void runItemProbInCollection(Path itemSum, double totalSum,
	    Path itemColl) throws ClassNotFoundException, IOException,
	    InterruptedException {

	Job job = new Job(getConf(), "RM2-4");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, itemSum);

	job.setMapperClass(ItemProbInCollectionMapper.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);

	job.setNumReduceTasks(0);

	job.setOutputFormatClass(MapFileOutputFormat.class);
	MapFileOutputFormat.setOutputPath(job, itemColl);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(DoubleWritable.class);

	Configuration conf = job.getConfiguration();
	conf.set(totalSumName, String.valueOf(totalSum));

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Calculates the item recommendation scores.
     * 
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void runItemRecommendation(Path userSum, Path clusteringCount,
	    Path clustering, Path itemColl) throws ClassNotFoundException,
	    IOException, InterruptedException {

	Job job = new Job(getConf(), "RM2-5");
	job.setJarByClass(this.getClass());

	Configuration conf = job.getConfiguration();
	MultipleInputs.addInputPath(job, userSum,
		SequenceFileInputFormat.class, UserSumByClusterMapper.class);

	if (conf.getBoolean("useCassandra", true)) {
	    MultipleInputs.addInputPath(job, new Path("unused"),
		    CqlPagingInputFormat.class,
		    ScoreByClusterCassandraMapper.class);
	    CassandraSetup.updateConfForInput(getConf(), conf);

	    job.setReducerClass(RM2CassandraReducer.class);
	    job.setOutputFormatClass(CqlOutputFormat.class);
	    job.setOutputKeyClass(Map.class);
	    job.setOutputValueClass(List.class);
	    CassandraSetup.updateConfForOutput(getConf(), conf);
	} else {
	    MultipleInputs.addInputPath(job, inputPath,
		    SequenceFileInputFormat.class,
		    ScoreByClusterHDFSMapper.class);

	    job.setReducerClass(RM2HDFSReducer.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(IntPairWritable.class);
	    job.setOutputValueClass(FloatWritable.class);
	    CassandraSetup.updateConfForOutput(getConf(), conf);
	}

	job.setMapOutputKeyClass(IntPairWritable.class);
	job.setMapOutputValueClass(IntDoubleOrPrefWritable.class);

	job.setPartitionerClass(IntPairKeyPartitioner.class);
	job.setSortComparatorClass(IntPairWritable.Comparator.class);
	job.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

	// Distributed cache
	Path mergedClustering = HDFSUtils.mergeFile(conf, clustering,
		directory, "clustering-merged");
	DistributedCache.addCacheFile(mergedClustering.toUri(), conf);

	Path mergedClusteringCount = HDFSUtils.mergeFile(conf, clusteringCount,
		directory, "clustering-count-merged");
	DistributedCache.addCacheFile(mergedClusteringCount.toUri(), conf);

	DistributedCache.addCacheFile(itemColl.toUri(), conf);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

}
