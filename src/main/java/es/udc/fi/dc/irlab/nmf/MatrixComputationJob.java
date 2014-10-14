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

package es.udc.fi.dc.irlab.nmf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;

/**
 * Abstraction of a matrix computation job for NMF or PPC.
 * 
 */
public abstract class MatrixComputationJob extends AbstractJob {

	public static final String numberOfFiles = "numberOfFiles";
	protected static final String maxSplitSize = "mapred.max.split.size";
	protected static final String minSplitSize = "mapred.min.split.size";
	protected static final String dfsSize = "dfs.block.size";
	protected static final String javaOpts = "mapred.child.java.opts";
	protected static final String GB5gc = "-Xmx5000m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC";
	protected static final String MB4096 = "4294967296";
	protected static final String MB16 = "16777216";
	protected static final String MB8 = "8388608";
	protected static final String MB4 = "4194304";

	protected String directory;
	protected int iteration;
	protected String cluster = "";

	protected Path H;
	protected Path W;
	protected Path H2;
	protected Path W2;
	protected Path out1;
	protected Path X;
	protected Path C;
	protected Path Y;

	/**
	 * MatrixComputationJob constructor.
	 * 
	 * @param H
	 *            matrix path
	 * @param W
	 *            matrix path
	 * @param H2
	 *            matrix path
	 * @param W2
	 *            matrix path
	 */
	public MatrixComputationJob(Path H, Path W, Path H2, Path W2) {
		super();
		this.H = H;
		this.W = W;
		this.H2 = H2;
		this.W2 = W2;
	}

	/**
	 * Add user mapping to distributed cache if necessary.
	 * 
	 * @param job
	 *            Job
	 * @param conf
	 *            Configuration
	 * @param mapItems
	 *            true if items need to be also mapped, false otherwise
	 * @throws IOException
	 */
	protected void injectMappings(Job job, Configuration conf, boolean mapItems)
			throws IOException {

		final int cluster = conf.getInt(RMRecommenderDriver.subClustering, -1);
		if (cluster < 0) {
			return;
		}

		Path folder = new Path(conf.get(RMRecommenderDriver.directory)
				+ File.separator + RMRecommenderDriver.subClusteringUserPath
				+ File.separator + RMRecommenderDriver.mappingPath);
		addMapping(job, conf, cluster, folder);

		if (mapItems) {
			folder = new Path(conf.get(RMRecommenderDriver.directory)
					+ File.separator
					+ RMRecommenderDriver.subClusteringItemPath
					+ File.separator + RMRecommenderDriver.mappingPath);
			addMapping(job, conf, cluster, folder);
		}

	}

	/**
	 * Auxiliary method. Add mapping from given folder.
	 * 
	 * @param job
	 *            Job
	 * @param conf
	 *            Configuration
	 * @param cluster
	 *            cluster id
	 * @param folder
	 *            data directory
	 * @throws IOException
	 */
	private void addMapping(Job job, Configuration conf, final int cluster,
			Path folder) throws IOException {

		FileSystem fs = FileSystem.get(conf);

		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(final Path path) {
				final String name = path.getName();
				return name.startsWith(cluster + "-r-");
			}
		};

		Path[] paths = FileUtil.stat2Paths(fs.listStatus(folder, filter));

		if (paths.length != 1) {
			throw new FileNotFoundException(folder + " not found!");
		}

		Configuration jobConf = job.getConfiguration();
		DistributedCache.addCacheFile(paths[0].toUri(), jobConf);

	}

}
