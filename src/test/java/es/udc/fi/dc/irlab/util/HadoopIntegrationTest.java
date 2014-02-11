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

package es.udc.fi.dc.irlab.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.primitives.Ints;

/**
 * Integration test class utility
 * 
 */
public abstract class HadoopIntegrationTest {

    protected static final double accuracy = 0.0001;

    protected String baseDirectory = "integrationTest";

    protected int cassandraPort = Integer.parseInt(System
	    .getenv("CASSANDRA_PORT"));
    protected String cassandraHost = System.getenv("CASSANDRA_HOST");
    protected String cassandraPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    protected String cassandraKeyspace = "recommendertest";
    protected String cassandraTable = "ratings";

    /**
     * Build basic configuration
     * 
     * @return Configuration object
     */
    protected Configuration buildConf() {
	Configuration conf = new Configuration();

	conf.set("directory", baseDirectory);
	conf.setInt("cassandraPort", cassandraPort);
	conf.set("cassandraHost", cassandraHost);
	conf.set("cassandraKeyspace", cassandraKeyspace);
	conf.set("cassandraPartitioner", cassandraPartitioner);
	conf.set("cassandraTable", cassandraTable);

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

	conf.setInt("numberOfUsers", numberOfUsers);
	conf.setInt("numberOfItems", numberOfItems);
	conf.setInt("numberOfClusters", numberOfClusters);
	conf.setInt("numberOfIterations", numberOfIterations);

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

    /**
     * Compare the data matrix with the data stored on given path (as
     * VectorWritable).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareMatrixData(double[][] data, String baseDirectory,
	    Path path) throws IOException {

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    IntWritable key = new IntWritable();
	    VectorWritable val = new VectorWritable();

	    int count = 0;

	    while (reader.next(key, val)) {
		count++;
		for (Vector.Element element : val.get().all()) {
		    assertEquals(data[key.get() - 1][element.index()],
			    element.get(), accuracy);
		}
	    }

	    if (count != data.length) {
		fail("Data length does not match");
	    }

	}

	fs.delete(mergedFile, false);

    }

    /**
     * Compare the data vector with the data stored on given path (as
     * DoubleWritable).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareVectorData(double[] data, String baseDirectory,
	    Path path) throws IOException {

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    IntWritable key = new IntWritable();
	    DoubleWritable val = new DoubleWritable();

	    int count = 0;

	    while (reader.next(key, val)) {
		count++;
		assertEquals(data[key.get() - 1], val.get(), accuracy);
	    }

	    if (count != data.length) {
		fail("Data length does not match");
	    }

	}

	fs.delete(mergedFile, false);

    }

    /**
     * Compare the data scalar with the data stored on given path (as
     * FloatWritable).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareScalarData(double data, String baseDirectory,
	    Path path) throws IOException {

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    NullWritable key = NullWritable.get();
	    DoubleWritable val = new DoubleWritable();

	    if (reader.next(key, val)) {
		assertEquals(data, val.get(), accuracy);
	    } else {
		fail("Data length does not match");
	    }
	}

	fs.delete(mergedFile, false);

    }

    /**
     * Compare the data matrix with the data stored on given path (as
     * IntSetWritable).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareIntSetData(int[][] data, String baseDirectory,
	    Path path) throws IOException {

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    IntWritable key = new IntWritable();
	    IntSetWritable val = new IntSetWritable();

	    int count = 0;

	    while (reader.next(key, val)) {
		count++;

		Set<Integer> set = new HashSet<Integer>(Ints.asList(data[key
			.get() - 1]));

		assertEquals(set, val.get());
	    }

	    if (count != data.length) {
		fail("Data length does not match");
	    }

	}

	fs.delete(mergedFile, false);

    }
}
