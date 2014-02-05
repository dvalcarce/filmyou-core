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
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.primitives.Ints;

/**
 * Integration test class utility
 * 
 */
public abstract class HadoopIntegrationTest {

    protected String baseDirectory = "integrationTest";

    /**
     * Delete old data
     * 
     * @throws IOException
     */
    protected void deletePreviousData(Configuration conf) throws IOException {
	FileSystem fs = FileSystem.get(URI.create(baseDirectory), conf);
	Path path = new Path(baseDirectory);
	fs.delete(path, true);
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
    protected void compareVectorData(double[][] data, String baseDirectory,
	    Path path) throws IOException {

	double accuracy = 0.0001;

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
