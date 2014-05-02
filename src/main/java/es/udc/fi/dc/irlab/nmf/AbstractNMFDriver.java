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
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.DataInitialization;

/**
 * Abstract NMF algorithm driver.
 * 
 */
public abstract class AbstractNMFDriver extends AbstractJob {

	private static final Log LOG = LogFactory.getLog(AbstractNMFDriver.class);

	private Class<? extends MatrixComputationJob> hClass;
	private Class<? extends MatrixComputationJob> wClass;

	private int numberOfUsers;
	private int numberOfItems;
	private int numberOfClusters;
	private int numberOfIterations;

	private String baseDirectory;

	private Path H;
	private Path W;
	private Path H2;
	private Path W2;

	/**
	 * AbstractNMFDriver constructor.
	 * 
	 * @param hClass
	 *            class of a MatrixComputationJob for h computation
	 * @param wClass
	 *            class of a MatrixComputationJob for w computation
	 */
	public AbstractNMFDriver(Class<? extends MatrixComputationJob> hClass,
			Class<? extends MatrixComputationJob> wClass) {
		this.hClass = hClass;
		this.wClass = wClass;
	}

	/**
	 * Create random initial data for H and W.
	 * 
	 * @throws IOException
	 */
	protected void createInitialMatrices() throws IOException {
		LOG.info("Creating H (" + numberOfUsers + "x" + numberOfClusters + ")");
		H = DataInitialization.createMatrix(getConf(), baseDirectory, "H",
				numberOfUsers, numberOfClusters);

		LOG.info("Creating W(" + numberOfItems + "x" + numberOfClusters + ")");
		W = DataInitialization.createMatrix(getConf(), baseDirectory, "W",
				numberOfItems, numberOfClusters);
	}

	/**
	 * Compute H and W matrices.
	 * 
	 * @param args
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		numberOfUsers = conf.getInt(RMRecommenderDriver.numberOfUsers, 0);
		numberOfItems = conf.getInt(RMRecommenderDriver.numberOfItems, 0);
		numberOfClusters = conf.getInt(RMRecommenderDriver.numberOfClusters, 0);
		numberOfIterations = conf.getInt(
				RMRecommenderDriver.numberOfIterations, 0);

		baseDirectory = conf.get(RMRecommenderDriver.directory);

		/* Matrix initialization */
		if (conf.get(RMRecommenderDriver.H) != null
				&& conf.get(RMRecommenderDriver.H).length() > 0) {
			H = new Path(conf.get(RMRecommenderDriver.H));
			W = new Path(conf.get(RMRecommenderDriver.W));
		} else {
			createInitialMatrices();
		}
		H2 = new Path(baseDirectory + File.separator + "H2");
		W2 = new Path(baseDirectory + File.separator + "W2");
		FileSystem fs = H.getFileSystem(conf);

		LOG.info("Running matrix factorization");

		/* Run algorithm iterations */
		for (int i = 1; i <= numberOfIterations; i++) {
			MatrixComputationJob hJob = hClass.getConstructor(
					new Class[] { Path.class, Path.class, Path.class,
							Path.class }).newInstance(H, W, H2, W2);

			MatrixComputationJob wJob = wClass.getConstructor(
					new Class[] { Path.class, Path.class, Path.class,
							Path.class }).newInstance(H, W, H2, W2);

			conf.setInt(RMRecommenderDriver.iteration, i);
			LOG.info("Launching H" + i);
			ToolRunner.run(conf, hJob, args);
			LOG.info("Launching W" + i);
			ToolRunner.run(conf, wJob, args);

			fs.delete(H, true);
			fs.delete(W, true);
			FileUtil.copy(fs, H2, fs, H, false, false, getConf());
			FileUtil.copy(fs, W2, fs, W, false, false, getConf());
			fs.delete(H2, true);
			fs.delete(W2, true);
		}

		return 0;
	}

}
