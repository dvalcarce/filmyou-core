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

import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.AbstractJob;

abstract public class MatrixComputationJob extends AbstractJob implements Tool {

    /**
     * Load default command line arguments.
     */
    protected void loadDefaultSetup() {
	addOption("numberOfUsers", "n", "Number of users", true);
	addOption("numberOfItems", "m", "Number of movies", true);
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

}
