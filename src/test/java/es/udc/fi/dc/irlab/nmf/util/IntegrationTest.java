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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Integration test class utility
 * 
 */
public class IntegrationTest {

    protected String baseDirectory = "integrationTest";
    protected int numberOfIterations = 1;
    protected int cassandraPort = 9160;
    protected String cassandraHost = "127.0.0.1";
    protected String cassandraPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    protected String cassandraKeyspace = "recommendertest";
    protected String cassandraTable = "ratings";

    /**
     * Build command line arguments for jobs.
     * 
     * @param H
     *            H matrix path
     * @param W
     *            W matrix path
     * @return
     */
    protected String[] buildArgs(Path H, Path W) {
	String[] result = new String[] { "--numberOfUsers",
		String.valueOf(NMFTestData.numberOfUsers), "--numberOfItems",
		String.valueOf(NMFTestData.numberOfItems),
		"--numberOfClusters",
		String.valueOf(NMFTestData.numberOfClusters),
		"--numberOfIterations", String.valueOf(numberOfIterations),
		"--directory", baseDirectory, "--cassandraPort",
		String.valueOf(cassandraPort), "--cassandraHost",
		cassandraHost, "--cassandraKeyspace", cassandraKeyspace,
		"--cassandraTable", cassandraTable, "--cassandraPartitioner",
		cassandraPartitioner, "--H", H.toString(), "--W", W.toString() };
	return result;

    }

    protected void deletePreviousData() throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(baseDirectory), conf);
	Path path = new Path(baseDirectory);
	fs.delete(path, true);
    }

    protected void compareData(double[][] data, String baseDirectory, Path path)
	    throws IOException {
	double accuracy = 0.0001;
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    IntWritable key = new IntWritable();
	    VectorWritable val = new VectorWritable();

	    if (reader.next(key, val)) {
		for (Vector.Element element : val.get().all()) {
		    assertEquals(data[key.get() - 1][element.index()],
			    element.get(), accuracy);
		}
	    } else {
		throw new RuntimeException(
			"assertEquals->reader.next() failed!");
	    }
	}

	fs.delete(mergedFile, false);
    }
}
