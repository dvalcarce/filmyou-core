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
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.AbstractJob;

abstract public class MatrixComputationJob extends AbstractJob implements Tool {

    protected String directory;

    protected Path H;
    protected Path W;
    protected Path H2;
    protected Path W2;
    protected Path out1;
    protected Path X;
    protected Path C;
    protected Path Y;

    public MatrixComputationJob(Path H, Path W, Path H2, Path W2) {
	this.H = H;
	this.W = W;
	this.H2 = H2;
	this.W2 = W2;
    }

    /**
     * Parse command line arguments.
     * 
     * @param args
     *            command line arguments
     * @throws IOException
     */
    protected void parseInput(String[] args) throws IOException {
	loadDefaultSetup();

	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    throw new IllegalArgumentException("Invalid arguments");
	}
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
     * Delete directory.
     * 
     * @throws IOException
     */
    protected void cleanPreviousData(String path) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(path), conf);
	fs.delete(new Path(path), true);
    }

}
