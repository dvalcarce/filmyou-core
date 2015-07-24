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

package es.udc.fi.dc.irlab.nmf.ppc.hcomputation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.nmf.common.Vector0Mapper;
import es.udc.fi.dc.irlab.nmf.common.Vector1Mapper;
import es.udc.fi.dc.irlab.nmf.common.Vector2Mapper;
import es.udc.fi.dc.irlab.nmf.hcomputation.ComputeHJob;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntPairKeyPartitioner;

public class PPCComputeHJob extends ComputeHJob {

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
    public PPCComputeHJob(final Path H, final Path W, final Path H2, final Path W2) {
        super(H, W, H2, W2);
        prefix = "PPC_";
    }

    /**
     * Launch the fifth job for H computation.
     *
     * @param hPath
     *            initial H path
     * @param xPath
     *            input X
     * @param yPath
     *            input Y
     * @param hOutputPath
     *            output H path
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Override
    protected void runJob4(final Path hPath, final Path xPath, final Path yPath,
            final Path hOutputPath)
                    throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = getConf();

        final Job job = new Job(HadoopUtils.sanitizeConf(conf),
                prefix + "H4" + cluster + "-it" + iteration);
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job, hPath, SequenceFileInputFormat.class, Vector0Mapper.class);
        MultipleInputs.addInputPath(job, xPath, SequenceFileInputFormat.class, Vector1Mapper.class);
        MultipleInputs.addInputPath(job, yPath, SequenceFileInputFormat.class, Vector2Mapper.class);

        job.setReducerClass(PPCHComputationReducer.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, hOutputPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setPartitionerClass(IntPairKeyPartitioner.class);
        job.setSortComparatorClass(IntPairWritable.Comparator.class);
        job.setGroupingComparatorClass(IntPairWritable.FirstGroupingComparator.class);

        final Configuration jobConf = job.getConfiguration();
        injectMappings(job, conf, false);
        jobConf.setInt(MatrixComputationJob.numberOfFiles, 1);

        final boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }

    }

}
