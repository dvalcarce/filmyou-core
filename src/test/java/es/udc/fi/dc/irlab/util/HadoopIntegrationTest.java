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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;

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
    protected String cassandraTableIn = "ratings";
    protected String cassandraTableOut = "recommendations";
    protected String cassandraTTL = "6000";

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
	conf.set("cassandraTableIn", cassandraTableIn);
	conf.set("cassandraTableOut", cassandraTableOut);
	conf.set("cassandraTTL", cassandraTTL);
	conf.setFloat("lambda", 0.5f);

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
     * @return conf
     */
    protected Configuration buildConf(Path H, Path clustering,
	    Path clusteringCount, int numberOfUsers, int numberOfClusters) {

	Configuration conf = buildConf(H, null, numberOfUsers, 0,
		numberOfClusters, 0);

	if (clustering != null) {
	    conf.set("clustering", clustering.toString());
	}

	if (clusteringCount != null) {
	    conf.set("clusteringCount", clusteringCount.toString());
	}

	return conf;

    }

    /**
     * Build configuration object for RM2 Assignment job.
     * 
     * @param clustering
     *            clustering path
     * @param clusteringCount
     *            clusteringCount path
     * @param numberOfItems
     * @return conf
     */
    protected Configuration buildConf(Path clustering, Path clusteringCount,
	    int numberOfUsers, int numberOfItems) {

	Configuration conf = buildConf(null, null, numberOfUsers,
		numberOfItems, 0, 0);

	if (clustering != null) {
	    conf.set("clustering", clustering.toString());
	}

	if (clusteringCount != null) {
	    conf.set("clusteringCount", clusteringCount.toString());
	}

	return conf;

    }

    /**
     * Compare the data matrix with the data stored on given path (as
     * {@literal SequenceFile<IntWritable, VectorWritable>}).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareMatrixData(Configuration conf, double[][] data,
	    String baseDirectory, Path path) throws IOException {

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
     * {@literal SequenceFile<IntWritable, DoubleWritable>}).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareDoubleVectorData(Configuration conf, double[] data,
	    String baseDirectory, Path path) throws IOException {

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
     * Compare the data vector with the data stored on given path (as
     * {@literal MapFile<IntWritable, DoubleWritable>}).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareMapDoubleVectorData(Configuration conf,
	    double[] data, String baseDirectory, Path path) throws IOException {

	Reader[] readers = MapFileOutputFormat.getReaders(path, conf);
	Partitioner<IntWritable, DoubleWritable> partitioner = new HashPartitioner<IntWritable, DoubleWritable>();

	IntWritable key;
	DoubleWritable val = new DoubleWritable();

	for (int i = 1; i <= data.length; i++) {
	    key = new IntWritable(i);

	    if (MapFileOutputFormat.getEntry(readers, partitioner, key, val) == null) {
		fail(String.format("data %d not found", i));
	    }

	    assertEquals(data[i - 1], val.get(), accuracy);
	}

    }

    /**
     * Compare the data vector with the data stored on given path (as a MapFile
     * of {@literal <IntWritable, IntWritable>}).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareMapIntVectorData(Configuration conf, int[] data,
	    String baseDirectory, Path path) throws IOException {

	Reader[] readers = MapFileOutputFormat.getReaders(path, conf);
	Partitioner<IntWritable, IntWritable> partitioner = new HashPartitioner<IntWritable, IntWritable>();

	IntWritable key;
	IntWritable val = new IntWritable();

	for (int i = 1; i <= data.length; i++) {
	    key = new IntWritable(i);

	    if (MapFileOutputFormat.getEntry(readers, partitioner, key, val) == null) {
		fail(String.format("data %d not found", i));
	    }

	    assertEquals(data[i - 1], val.get());
	}

    }

    /**
     * Compare the data scalar with the data stored on given path (as a
     * {@literal SequenceFile<NullWritable, FloatWritable>}).
     * 
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareScalarData(Configuration conf, double data,
	    String baseDirectory, Path path) throws IOException {

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

    protected void compareCassandraData(Configuration conf, double[][] data,
	    int numberOfUsers) throws InterruptedException {

	ResultSet result;
	String keyspace = conf.get("cassandraKeyspace");
	String table = conf.get("cassandraTableOut");
	CassandraUtils cassandra = new CassandraUtils(
		conf.get("cassandraHost"), conf.get("cassandraPartitioner"));

	int i = 0;
	for (int user = 1; user <= numberOfUsers; user++) {
	    result = cassandra.selectData(user, keyspace, table);
	    for (Row row : result) {
		try {
		    assertEquals((int) data[i][0], row.getInt(0));
		    assertEquals((int) data[i][1], row.getInt(1));
		    assertEquals(data[i][2], row.getFloat(2), accuracy);
		} catch (AssertionError e) {
		    System.out.println("row: " + row);
		    System.out.println(String.format("data[%d]: %f, %f, %f", i,
			    data[i][0], data[i][1], data[i][2]));
		    throw e;
		}
		i++;
	    }
	}

	cassandra.shutdownSessions();

    }
}
