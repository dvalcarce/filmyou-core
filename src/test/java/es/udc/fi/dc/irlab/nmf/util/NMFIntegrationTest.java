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

package es.udc.fi.dc.irlab.nmf.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;

/**
 * Integration test class utility
 * 
 */
public abstract class NMFIntegrationTest extends HadoopIntegrationTest {

    protected int cassandraPort = Integer.parseInt(System
	    .getenv("CASSANDRA_PORT"));
    protected String cassandraHost = System.getenv("CASSANDRA_HOST");
    protected String cassandraPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    protected String cassandraKeyspace = "recommendertest";
    protected String cassandraTable = "rat";
    protected String hadoopHost = System.getenv("HADOOP_HOST");

    /**
     * Build basic configuration
     * 
     * @return Configuration object
     */
    protected Configuration buildConf() {
	Configuration conf = new Configuration();

	// conf.set("fs.default.name", "hdfs://" + hadoopHost + ":8020");
	// conf.set("mapred.job.tracker", hadoopHost + ":8021");
	// conf.set("dfs.namenode.rpc-address", hadoopHost);

	return conf;
    }

    /**
     * Build configuration object for NMF/PPC jobs.
     * 
     * @param H
     *            H matrix path
     * @param W
     *            W matrix path
     * @param numberOfUsers
     * @param numberOfItems
     * @param numberOfClusters
     * @param numberOfIterations
     * @return Configuration object
     */
    protected Configuration buildConf(Path H, Path W, int numberOfUsers,
	    int numberOfItems, int numberOfClusters, int numberOfIterations) {

	Configuration conf = buildConf();

	conf.set("directory", baseDirectory);

	conf.setInt("numberOfUsers", numberOfUsers);
	conf.setInt("numberOfItems", numberOfItems);
	conf.setInt("numberOfClusters", numberOfClusters);
	conf.setInt("numberOfIterations", numberOfIterations);

	conf.setInt("cassandraPort", cassandraPort);
	conf.set("cassandraHost", cassandraHost);
	conf.set("cassandraKeyspace", cassandraKeyspace);
	conf.set("cassandraPartitioner", cassandraPartitioner);
	conf.set("cassandraTable", cassandraTable);

	if (H != null) {
	    conf.set("H", H.toString());
	}
	if (W != null) {
	    conf.set("W", W.toString());
	}

	return conf;

    }

    /**
     * Build configuration object for Clustering Assignment job.
     * 
     * @param H
     *            H matrix path
     * @param clustering
     *            clustering path
     * @param numberOfUsers
     * @param numberOfClusters
     * @return
     */
    protected Configuration buildConf(Path H, Path clustering,
	    int numberOfUsers, int numberOfClusters) {

	Configuration conf = buildConf(H, null, numberOfUsers, 0,
		numberOfClusters, 0);

	if (clustering != null) {
	    conf.set("clustering", clustering.toString());
	}

	return conf;

    }

}
