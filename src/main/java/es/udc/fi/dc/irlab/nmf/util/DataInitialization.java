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

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Utility class for initializing random structures in HDFS.
 * 
 */
public class DataInitialization {

    private DataInitialization() {

    }

    /**
     * Create a (rows x cols) matrix of random values. Each row is normalized
     * according to the L_1 norm.
     * 
     * @param conf
     *            Configuration file
     * @param baseDirectory
     *            working directory
     * @param filename
     *            name of the file where the matrix is going to be stored.
     * @param rows
     *            number of rows
     * @param cols
     *            number of columns
     * @return Path of the matrix
     * @throws IOException
     */
    public static Path createMatrix(Configuration conf, String baseDirectory,
	    String filename, int rows, int cols) throws IOException {

	String uri = baseDirectory + "/" + filename;
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	Path path = new Path(uri);

	SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
		IntWritable.class, VectorWritable.class);

	Vector vector = new DenseVector(cols);
	Random randomGenerator = new Random();
	try {
	    for (int i = 1; i <= rows; i++) {
		for (int j = 0; j < cols; j++) {
		    vector.setQuick(j, randomGenerator.nextDouble());
		}
		vector = vector.normalize(1);
		writer.append(new IntWritable(i), new VectorWritable(vector));
	    }
	} finally {
	    IOUtils.closeStream(writer);
	}

	return path;

    }

    /**
     * Create a (rows x cols) matrix from double data.
     * 
     * @param conf
     *            Configuration file
     * @param data
     *            matrix data
     * @param baseDirectory
     *            working directory
     * @param filename
     *            name of the file where the matrix is going to be stored.
     * @return Path of the matrix
     * @throws IOException
     */
    public static Path createDoubleMatrix(Configuration conf, double[][] data,
	    String baseDirectory, String filename) throws IOException {

	String uri = baseDirectory + "/" + filename;
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	Path path = new Path(uri);

	SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
		IntWritable.class, VectorWritable.class);

	int cols = data[0].length;
	Vector vector = new DenseVector(cols);
	int i = 1;
	try {
	    for (double[] row : data) {
		vector.assign(row);
		writer.append(new IntWritable(i), new VectorWritable(vector));
		i++;
	    }
	} finally {
	    IOUtils.closeStream(writer);
	}

	return path;

    }

    /**
     * Create a MapFile&lt;IntWritable, IntWritable> from int[] data.
     * 
     * @param conf
     *            Configuration file
     * @param data
     *            vector data
     * @param baseDirectory
     *            working directory
     * @param filename
     *            name of the file where the matrix is going to be stored.
     * @return Path of the matrix
     * @throws IOException
     */
    public static Path createMapIntVector(Configuration conf, int[] data,
	    String baseDirectory, String filename) throws IOException {

	String parentUri = baseDirectory + "/" + filename;
	String uri = parentUri + "/data";
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	Path path = new Path(uri);
	Path parent = new Path(parentUri);

	MapFile.Writer writer = null;

	try {
	    writer = new MapFile.Writer(conf, fs, path.toString(),
		    IntWritable.class, IntWritable.class);

	    for (int i = 1; i <= data.length; i++) {
		writer.append(new IntWritable(i), new IntWritable(data[i - 1]));
	    }
	} finally {
	    IOUtils.closeStream(writer);
	}

	return parent;

    }
}
