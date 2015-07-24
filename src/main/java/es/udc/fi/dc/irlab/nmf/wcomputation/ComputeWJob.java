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

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.nmf.common.CrossProductMapper;
import es.udc.fi.dc.irlab.nmf.common.MatrixSumReducer;
import es.udc.fi.dc.irlab.nmf.common.Vector0Mapper;
import es.udc.fi.dc.irlab.nmf.common.VectorSumReducer;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.CassandraSetup;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntPairKeyPartitioner;

public class ComputeWJob extends MatrixComputationJob {

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
    public ComputeWJob(final Path H, final Path W, final Path H2, final Path W2) {
        super(H, W, H2, W2);
    }

    /**
     * Run all chained map-reduce jobs in order to compute H matrix.
     */
    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        inputPath = HadoopUtils.getInputPath(conf);
        int c;
        if ((c = conf.getInt(RMRecommenderDriver.subClustering, -1)) >= 0) {
            cluster = "(" + c + ")";
        }
        iteration = conf.getInt(RMRecommenderDriver.iteration, -1);
        directory = conf.get(RMRecommenderDriver.directory) + File.separator + "wcomputation";

        HadoopUtils.createFolder(conf, directory);
        HadoopUtils.removeData(conf, W2.toString());
        HadoopUtils.removeData(conf, directory);

        this.out1 = new Path(directory + File.separator + "wout1");
        this.X = new Path(directory + File.separator + "X");
        this.C = new Path(directory + File.separator + "C");
        this.Y = new Path(directory + File.separator + "Y");

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
     * @param hPath
     *            initial H path
     * @param wout1Path
     *            temporal output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob1(final Path hPath, final Path wout1Path)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = getConf();

        final Job job = new Job(HadoopUtils.sanitizeConf(conf), "W1" + cluster + "-it" + iteration);
        job.setJarByClass(ComputeWJob.class);

        final Configuration jobConf = job.getConfiguration();

        if (conf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
            MultipleInputs.addInputPath(job, new Path("unused"), CqlInputFormat.class,
                    ItemScoreByUserCassandraMapper.class);
            CassandraSetup.updateConfForInput(conf, jobConf);
        } else {
            MultipleInputs.addInputPath(job, inputPath, SequenceFileInputFormat.class,
                    ItemScoreByUserHDFSMapper.class);
        }

        MultipleInputs.addInputPath(job, hPath, SequenceFileInputFormat.class,
                HSecondaryMapper.class);

        job.setReducerClass(W1Reducer.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(VectorOrPrefWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, wout1Path);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setPartitionerClass(IntPairKeyPartitioner.class);
        job.setSortComparatorClass(IntPairWritable.Comparator.class);
        job.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

        injectMappings(job, conf, true);
        jobConf.setInt(MatrixComputationJob.numberOfFiles, 2);

        jobConf.set(dfsSize, MB4096);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

    /**
     * Launch the second job for W computation.
     *
     * @param wout1Path
     *            output of the first job
     * @param xPath
     *            X path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob2(final Path wout1Path, final Path xPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Job job = new Job(HadoopUtils.sanitizeConf(getConf()),
                "W2" + cluster + "-it" + iteration);
        job.setJarByClass(ComputeWJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, wout1Path);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(VectorSumReducer.class);
        job.setReducerClass(VectorSumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, xPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

    /**
     * Launch the third job for W computation.
     *
     * @param wPath
     *            W path
     * @param cPath
     *            C path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob3(final Path wPath, final Path cPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Job job = new Job(HadoopUtils.sanitizeConf(getConf()),
                "W3" + cluster + "-it" + iteration);
        job.setJarByClass(ComputeWJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, wPath);

        job.setMapperClass(CrossProductMapper.class);
        job.setCombinerClass(MatrixSumReducer.class);
        job.setReducerClass(MatrixSumReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MatrixWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, cPath);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatrixWritable.class);

        final Configuration jobConf = job.getConfiguration();
        jobConf.set(maxSplitSize, MB4);
        jobConf.set(javaOpts, GB5gc);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

    /**
     * Launch the fourth job for H computation.
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
    protected void runJob4(final Path hPath, final Path yPath, final Path cPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        final Job job = new Job(HadoopUtils.sanitizeConf(getConf()),
                "W4" + cluster + "-it" + iteration);
        job.setJarByClass(ComputeWJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, hPath);

        job.setMapperClass(WCMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, yPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        final Configuration jobConf = job.getConfiguration();
        DistributedCache.addCacheFile(cPath.toUri(), jobConf);
        jobConf.setInt(MatrixComputationJob.numberOfFiles, 1);

        jobConf.set(maxSplitSize, MB4);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

    /**
     * Launch the fifth job for H computation.
     *
     * @param wPath
     *            initial W path
     * @param xPath
     *            input X
     * @param yPath
     *            input Y
     * @param wOutputPath
     *            output W path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void runJob5(final Path wPath, final Path xPath, final Path yPath,
            final Path wOutputPath)
                    throws IOException, ClassNotFoundException, InterruptedException {

        final Job job = new Job(HadoopUtils.sanitizeConf(getConf()),
                "W5" + cluster + "-it" + iteration);
        job.setJarByClass(ComputeWJob.class);

        MultipleInputs.addInputPath(job, wPath, SequenceFileInputFormat.class, Vector0Mapper.class);

        job.setMapperClass(WComputationMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, wOutputPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        final Configuration jobConf = job.getConfiguration();
        DistributedCache.addCacheFile(xPath.toUri(), jobConf);
        DistributedCache.addCacheFile(yPath.toUri(), jobConf);
        jobConf.setInt(MatrixComputationJob.numberOfFiles, 2);

        jobConf.set(maxSplitSize, MB4);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

}
