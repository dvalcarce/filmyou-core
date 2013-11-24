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

package es.udc.fi.dc.irlab.ppc;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.ppc.hcomputation.ComputeHJob;
import es.udc.fi.dc.irlab.ppc.util.DataInitialization;
import es.udc.fi.dc.irlab.ppc.wcomputation.ComputeWJob;

/**
 * PPC algorithm driver.
 * 
 */
public class PPCDriver extends AbstractJob {

    private static int numberOfUsers;
    private static int numberOfItems;
    private static int numberOfClusters;
    private static int numberOfIterations;

    /**
     * Load default command line arguments.
     */
    protected void loadDefaultSetup() {
	addOption("numberOfUsers", "m", "Number of users", true);
	addOption("numberOfItems", "n", "Number of movies", true);
	addOption("numberOfClusters", "k", "Number of PPC clusters", true);
	addOption("numberOfIterations", "i", "Number of PPC iterations", "1");
	addOption("directory", "d", "Working directory", "ppc");
	addOption("cassandraPort", "port", "Cassandra TCP port", "9160");
	addOption("cassandraHost", "host", "Cassandra host IP", "127.0.0.1");
	addOption("cassandraKeyspace", "keyspace", "Cassandra keyspace name",
		true);
	addOption("cassandraTable", "table", "Cassandra Column Family name",
		true);
	addOption("cassandraPartitioner", "partitioner",
		"Cassandra Partitioner",
		"org.apache.cassandra.dht.Murmur3Partitioner");
    }

    /**
     * Compute H and W matrices.
     * 
     */
    @Override
    public int run(String[] args) throws Exception {
	loadDefaultSetup();

	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    throw new IllegalArgumentException("Invalid arguments");
	}

	numberOfUsers = Integer.parseInt(getOption("numberOfUsers"));
	numberOfItems = Integer.parseInt(getOption("numberOfItems"));
	numberOfClusters = Integer.parseInt(getOption("numberOfClusters"));
	numberOfIterations = Integer.parseInt(getOption("numberOfIterations"));

	String baseDirectory = getOption("directory");

	Path H = DataInitialization.createMatrix(baseDirectory, "H",
		numberOfUsers, numberOfClusters);
	Path W = DataInitialization.createMatrix(baseDirectory, "W",
		numberOfItems, numberOfClusters);

	for (int i = 0; i < numberOfIterations; i++) {
	    ToolRunner.run(new Configuration(), new ComputeHJob(H), args);
	    ToolRunner.run(new Configuration(), new ComputeWJob(W), args);
	}

	return 0;
    }
}
