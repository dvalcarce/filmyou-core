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

package es.udc.fi.dc.irlab.ppc.wcomputation;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.AbstractJob;

public class ComputeWJob extends AbstractJob implements Tool {

    private final Path H;
    private final Path W;
    private Path out1;

    /**
     * ComputeWJob constructor.
     * 
     * @param H
     *            Path to the H matrix
     * @param W
     *            Path to the W matrix
     */
    public ComputeWJob(Path H, Path W) {
	this.H = H;
	this.W = H;
    }

    /**
     * Run all chained map-reduce jobs in order to compute H matrix.
     */
    @Override
    public int run(String[] args) throws Exception {
	parseArguments(args, true, true);

	String directory = getOption("directory");
	this.out1 = new Path(directory + "/out1");

	// runJob1(W, out1);

	return 0;
    }

    /**
     * Launch the first job for W computation.
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

	Job job = new Job(getConf(), "Job W1");
	job.setJarByClass(ComputeWJob.class);

	// job.setInputFormatClass(.class);
	// .setInputPath(job, inputPath);
	//
	// job.setReducerClass(H1Reducer.class);
	//
	// job.setMapOutputKeyClass(.class);
	// job.setMapOutputValueClass(.class);
	//
	// job.setOutputKeyClass(.class);
	// job.setOutputValueClass(.class);
	//
	// job.setOutputFormatClass(.class);
	// .setOutputPath(job, outputPath);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }
}
