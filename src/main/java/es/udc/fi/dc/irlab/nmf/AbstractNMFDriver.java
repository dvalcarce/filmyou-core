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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.nmf.util.DataInitialization;

/**
 * Abstract NMF algorithm driver.
 * 
 */
public abstract class AbstractNMFDriver extends AbstractJob {

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

    public AbstractNMFDriver(Class<? extends MatrixComputationJob> hClass,
	    Class<? extends MatrixComputationJob> wClass) {
	this.hClass = hClass;
	this.wClass = wClass;
    }

    /**
     * Load default command line arguments.
     */
    protected void loadDefaultSetup() {
	addOption("numberOfUsers", "n", "Number of users", true);
	addOption("numberOfItems", "m", "Number of movies", true);
	addOption("numberOfClusters", "k", "Number of clusters", true);
	addOption("numberOfIterations", "i", "Number of iterations", "1");
	addOption("directory", "d", "Working directory", "clustering");
	addOption("cassandraPort", "port", "Cassandra TCP port", "9160");
	addOption("cassandraHost", "host", "Cassandra host IP", "127.0.0.1");
	addOption("cassandraKeyspace", "keyspace", "Cassandra keyspace name",
		true);
	addOption("cassandraTable", "table", "Cassandra Column Family name",
		true);
	addOption("cassandraPartitioner", "partitioner",
		"Cassandra Partitioner",
		"org.apache.cassandra.dht.Murmur3Partitioner");
	addOption("H", "h", "Initial H matrix", false);
	addOption("W", "w", "Initial W matrix", false);
    }

    /**
     * Create random initial data for H and W.
     * 
     * @throws IOException
     */
    protected void createInitialMatrices() throws IOException {
	H = DataInitialization.createMatrix(baseDirectory, "H", numberOfUsers,
		numberOfClusters);

	W = DataInitialization.createMatrix(baseDirectory, "W", numberOfItems,
		numberOfClusters);
    }

    /**
     * Compute H and W matrices.
     * 
     */
    @Override
    public int run(String[] args) throws Exception {
	/* Parse input */
	loadDefaultSetup();
	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    throw new IllegalArgumentException("Invalid arguments");
	}

	numberOfUsers = Integer.parseInt(getOption("numberOfUsers"));
	numberOfItems = Integer.parseInt(getOption("numberOfItems"));
	numberOfClusters = Integer.parseInt(getOption("numberOfClusters"));
	numberOfIterations = Integer.parseInt(getOption("numberOfIterations"));

	baseDirectory = getOption("directory");

	/* Matrix initialisation */
	if (getOption("H") != null) {
	    H = new Path(getOption("H"));
	    W = new Path(getOption("W"));
	} else {
	    createInitialMatrices();
	}
	H2 = new Path(baseDirectory + "/H2");
	W2 = new Path(baseDirectory + "/W2");
	FileSystem fs = H.getFileSystem(getConf());

	/* Run algorithm */
	for (int i = 0; i < numberOfIterations; i++) {
	    MatrixComputationJob hJob = hClass.getConstructor(
		    new Class[] { Path.class, Path.class, Path.class,
			    Path.class }).newInstance(H, W, H2, W2);

	    MatrixComputationJob wJob = wClass.getConstructor(
		    new Class[] { Path.class, Path.class, Path.class,
			    Path.class }).newInstance(H, W, H2, W2);

	    ToolRunner.run(new Configuration(), wJob, args);
	    ToolRunner.run(new Configuration(), hJob, args);

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
