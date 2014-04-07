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

package es.udc.fi.dc.irlab.rmrecommender;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.nmf.clustering.ClusterAssignmentJob;
import es.udc.fi.dc.irlab.nmf.ppc.PPCDriver;
import es.udc.fi.dc.irlab.rm.RM2Job;

/**
 * Relevance Model Recommender
 * 
 */
public class RMRecommenderJob extends AbstractJob {

    public static final String useCassandra = "useCassandra";

    /**
     * Load default command line arguments.
     */
    private void loadDefaultSetup() {
	addOption("numberOfUsers", "n", "Number of users", true);
	addOption("numberOfItems", "m", "Number of movies", true);
	addOption("numberOfClusters", "k", "Number of clusters", true);
	addOption("numberOfIterations", "i", "Number of iterations", "50");
	addOption("directory", "d", "Working directory", "recommendation");
	addOption(useCassandra, "useCas", "Use Cassandra instead of HDFS",
		"true");
	addOption("cassandraPort", "port", "Cassandra TCP port", "9160");
	addOption("cassandraHost", "host", "Cassandra host IP", "127.0.0.1");
	addOption("cassandraKeyspace", "keyspace", "Cassandra keyspace name",
		"recommendertest");
	addOption("cassandraTableIn", "table",
		"Cassandra Column Family name for input data", "ratings");
	addOption("cassandraTableOut", "table",
		"Cassandra Column Family name for output data",
		"recommendations");
	addOption("cassandraPartitioner", "partitioner",
		"Cassandra Partitioner",
		"org.apache.cassandra.dht.Murmur3Partitioner");
	addOption("cassandraTTL", "ttl", "Cassandra TLL for data input",
		"86400");
	addOption("H", "h", "Initial H matrix", false);
	addOption("W", "w", "Initial W matrix", false);
	addOption("clustering", "cluster", "Clustering results", "clustering");
	addOption("clusteringCount", "clusterCount", "Clustering sizes",
		"clusteringCount");
	addOption("lambda", "l",
		"Lambda parameter for Jelinek-Mercer smoothing", "0.5");
    }

    /**
     * Parse command line arguments and update job configuration.
     * 
     * @param args
     *            command line arguments
     * @throws IOException
     */
    protected Configuration parseInput(String[] args) throws IOException {
	/* Add options */
	loadDefaultSetup();

	/* Parse arguments */
	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    throw new IllegalArgumentException("Invalid arguments");
	}

	/* Update job configuration */
	Configuration conf = getConf();

	for (Entry<String, List<String>> entry : parsedArgs.entrySet()) {
	    String key = entry.getKey();
	    for (String value : entry.getValue()) {
		conf.set(key.substring(2), value);
	    }
	}

	return conf;
    }

    /**
     * Main routine. Launch RMRecommender job.
     * 
     * @param args
     *            command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new RMRecommenderJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = parseInput(args);

	if (ToolRunner.run(conf, new PPCDriver(), args) < 0) {
	    throw new RuntimeException("PPCJob failed!");
	}

	if (conf.get("H") == null) {
	    String directory = conf.get("directory");
	    conf.set("H", directory + "/H");
	    conf.set("W", directory + "/W");
	}

	if (ToolRunner.run(conf, new ClusterAssignmentJob(), args) < 0) {
	    throw new RuntimeException("ClusterAssignmentJob failed!");
	}

	if (ToolRunner.run(conf, new RM2Job(), args) < 0) {
	    throw new RuntimeException("RMJob failed!");
	}

	return 0;
    }

}
