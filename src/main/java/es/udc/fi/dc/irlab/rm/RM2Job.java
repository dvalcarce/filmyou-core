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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.IntKeyPartitioner;
import es.udc.fi.dc.irlab.util.MapFileOutputFormat;
import es.udc.fi.dc.irlab.util.StringIntPairWritable;

/**
 * Relevance Model 2 job.
 *
 */
public class RM2Job extends AbstractJob {

    public enum Score {
        SUM
    }

    public static final long OFFSET = 100;

    public static final String TOTAL_SUM_NAME = "rm.totalSum";
    public static final String LAMBDA_NAME = "lambda";
    public static final String USER_SUM = "userSum";
    public static final String ITEM_SUM = "itemSum";
    public static final String TOTAL_SUM = "totalSum";
    public static final String ITEMM_COLL = "itemColl";

    private String directory;

    /**
     * Run all chained map-reduce jobs in order to compute RM2.
     *
     * @return int
     */
    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        inputPath = HadoopUtils.getInputPath(conf);
        outputPath = HadoopUtils.getOutputPath(conf);

        final String baseDirectory = conf.get(RMRecommenderDriver.directory);
        directory = baseDirectory + "/rm2";
        HadoopUtils.removeData(conf, directory);

        final Path userSum = new Path(directory + File.separator + RM2Job.USER_SUM);
        final Path itemColl = new Path(directory + File.separator + RM2Job.ITEMM_COLL);
        final Path clustering = new Path(
                baseDirectory + File.separator + conf.get(RMRecommenderDriver.clustering));
        final Path clusteringCount = new Path(
                baseDirectory + File.separator + conf.get(RMRecommenderDriver.clusteringCount));

        final double sum = runUserSum(userSum);

        runItemColl(sum / OFFSET, itemColl);

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
    protected double runUserSum(final Path userSum)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = getConf();

        final Job job = new Job(HadoopUtils.sanitizeConf(conf), "RM2-1");
        job.setJarByClass(this.getClass());

        final Configuration jobConf = job.getConfiguration();

        if (conf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
            job.setInputFormatClass(CqlInputFormat.class);
            CassandraSetup.updateConfForInput(conf, jobConf);
            job.setMapperClass(SimpleScoreByUserCassandraMapper.class);
        } else {
            job.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
            job.setMapperClass(SimpleScoreByUserHDFSMapper.class);
        }

        job.setCombinerClass(DoubleSumReducer.class);
        job.setReducerClass(DoubleSumAndCountReducer.class);

        job.setPartitionerClass(IntKeyPartitioner.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, userSum);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

        return job.getCounters().findCounter(Score.SUM).getValue();

    }

    /**
     * Calculates the probability of each item in the collection.
     *
     * @param totalSum
     *            sum of all the ratings
     * @param itemColl
     *            output Path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runItemColl(final double totalSum, final Path itemColl)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = getConf();

        final Job job = new Job(HadoopUtils.sanitizeConf(conf), "RM2-2");
        job.setJarByClass(this.getClass());

        final Configuration jobConf = job.getConfiguration();

        if (conf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
            job.setInputFormatClass(CqlInputFormat.class);
            CassandraSetup.updateConfForInput(conf, jobConf);
            job.setMapperClass(SimpleScoreByItemCassandraMapper.class);
        } else {
            job.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
            job.setMapperClass(SimpleScoreByItemHDFSMapper.class);
        }

        job.setCombinerClass(DoubleSumReducer.class);
        job.setReducerClass(DoubleSumAndDividerReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(IntKeyPartitioner.class);

        job.setOutputFormatClass(MapFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, itemColl);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        jobConf.set(TOTAL_SUM_NAME, String.valueOf(totalSum));

        final boolean succeeded = job.waitForCompletion(true);
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
    protected void runItemRecommendation(final Path userSum, final Path clusteringCount,
            final Path clustering, final Path itemColl)
                    throws ClassNotFoundException, IOException, InterruptedException {

        final Configuration conf = getConf();

        final Job job = new Job(HadoopUtils.sanitizeConf(conf), "RM2-3");
        job.setJarByClass(this.getClass());

        final Configuration jobConf = job.getConfiguration();

        MultipleInputs.addInputPath(job, userSum, SequenceFileInputFormat.class,
                UserSumByClusterMapper.class);

        if (jobConf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
            MultipleInputs.addInputPath(job, new Path("unused"), CqlInputFormat.class,
                    ScoreByClusterCassandraMapper.class);
            CassandraSetup.updateConfForInput(conf, jobConf);
        } else {
            MultipleInputs.addInputPath(job, inputPath, SequenceFileInputFormat.class,
                    ScoreByClusterHDFSMapper.class);
        }

        if (jobConf.getBoolean(RMRecommenderDriver.useCassandraOutput, true)) {
            job.setReducerClass(RM2CassandraReducer.class);
            job.setOutputFormatClass(CqlOutputFormat.class);
            job.setOutputKeyClass(Map.class);
            job.setOutputValueClass(List.class);
            CassandraSetup.updateConfForOutput(conf, jobConf);
        } else {
            job.setReducerClass(RM2HDFSReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(IntPairWritable.class);
            job.setOutputValueClass(FloatWritable.class);
            FileOutputFormat.setOutputPath(job, outputPath);
        }

        job.setNumReduceTasks(conf.getInt(RMRecommenderDriver.numberOfClusters, -1));

        job.setMapOutputKeyClass(StringIntPairWritable.class);
        job.setMapOutputValueClass(IntDoubleOrPrefWritable.class);

        job.setPartitionerClass(StringIntPairWritable.KeyPartitioner.class);
        job.setSortComparatorClass(StringIntPairWritable.Comparator.class);
        job.setGroupingComparatorClass(StringIntPairWritable.KeyComparator.class);

        DistributedCache.addCacheFile(clustering.toUri(), jobConf);
        DistributedCache.addCacheFile(clusteringCount.toUri(), jobConf);
        DistributedCache.addCacheFile(itemColl.toUri(), jobConf);
        jobConf.setInt(MatrixComputationJob.numberOfFiles, 3);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

}
