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

package es.udc.fi.dc.irlab.nmf.clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.util.HDFSUtils;
import es.udc.fi.dc.irlab.util.MapFileOutputFormat;

/**
 * Assign each user to a cluster after NMF/PPC execution.
 * 
 */
public class ClusterAssignmentJob extends AbstractJob {

    protected String directory;

    protected Path H;
    protected Path clustering;
    protected Path clusteringCount;

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();

	/* Prepare paths */
	String directory = conf.get("directory");
	H = new Path(conf.get("H"));
	clustering = new Path(directory + "/" + conf.get("clustering"));
	clusteringCount = new Path(directory + "/"
		+ conf.get("clusteringCount"));
	HDFSUtils.removeData(conf, clustering.toString());
	HDFSUtils.removeData(conf, clusteringCount.toString());

	/* Launch jobs */
	findClustersJob(H, clustering);

	countClusters(clustering, clusteringCount);

	return 0;
    }

    /**
     * Find the proper cluster for each user and group them by cluster.
     * 
     * @param inputPathH
     *            Path of H matrix (after NMF/PPC algorithm)
     * @param outputPath
     *            Clusters
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void findClustersJob(Path inputPathH, Path outputPath)
	    throws IOException, ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "FindClustersJob");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPathH);

	job.setMapperClass(FindClusterMapper.class);

	job.setNumReduceTasks(0);

	job.setOutputFormatClass(MapFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

    /**
     * Count the number of elements in each cluster.
     * 
     * @param inputPath
     *            Clustering assignments
     * @param outputPath
     *            Number of elements in each cluster
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected void countClusters(Path inputPath, Path outputPath)
	    throws IOException, ClassNotFoundException, InterruptedException {

	Job job = new Job(getConf(), "CountClustersJob");
	job.setJarByClass(this.getClass());

	job.setInputFormatClass(SequenceFileInputFormat.class);
	SequenceFileInputFormat.addInputPath(job, inputPath);

	job.setMapperClass(InverseMapper.class);
	job.setReducerClass(CountReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputFormatClass(MapFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputPath(job, outputPath);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean succeeded = job.waitForCompletion(true);
	if (!succeeded) {
	    throw new RuntimeException(job.getJobName() + " failed!");
	}

    }

}
