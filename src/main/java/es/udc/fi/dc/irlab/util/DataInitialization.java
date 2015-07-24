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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;

/**
 * Utility class for initializing random structures in HDFS.
 *
 */
public final class DataInitialization {

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
    public static Path createMatrix(final Configuration conf, final String baseDirectory,
            final String filename, final int rows, final int cols) throws IOException {

        final String uri = baseDirectory + File.separator + filename;
        final FileSystem fs = FileSystem.get(URI.create(uri), conf);
        final Path path = new Path(uri);

        HadoopUtils.removeData(conf, uri);
        final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
                IntWritable.class, VectorWritable.class);

        Vector vector = new DenseVector(cols);
        final Random randomGenerator = new Random();
        try {
            for (int i = 1; i <= rows; i++) {
                for (int j = 0; j < cols; j++) {
                    vector.setQuick(j, randomGenerator.nextDouble() + MatrixComputationJob.eps);
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
     * @param start
     *            start index
     * @return Path of the matrix
     * @throws IOException
     */
    public static Path createDoubleMatrix(final Configuration conf, final double[][] data,
            final String baseDirectory, final String filename, final int start) throws IOException {

        final String uri = baseDirectory + File.separator + filename;
        final FileSystem fs = FileSystem.get(URI.create(uri), conf);
        final Path path = new Path(uri);

        final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
                IntWritable.class, VectorWritable.class);

        final int cols = data[0].length;
        final Vector vector = new DenseVector(cols);
        int i = start;
        try {
            for (final double[] row : data) {
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
     * Create a SequenceFile&lt;IntPairWritable, FloatWritable> from double[][]
     * data.
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
    public static Path createIntPairFloatFile(final Configuration conf, final double[][] data,
            final String baseDirectory, final String filename) throws IOException {

        final String parentUri = baseDirectory + File.separator + filename;
        final String uri = parentUri + File.separator + "data";
        final FileSystem fs = FileSystem.get(URI.create(uri), conf);
        final Path path = new Path(uri);
        final Path parent = new Path(parentUri);

        final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
                IntPairWritable.class, FloatWritable.class);

        try {
            for (int i = 0; i < data.length; i++) {
                for (int j = 0; j < data[i].length; j++) {
                    if (data[i][j] > 0) {
                        writer.append(new IntPairWritable(j + 1, i + 1),
                                new FloatWritable((float) data[i][j]));
                    }
                }
            }
        } finally {
            IOUtils.closeStream(writer);
        }

        return parent;

    }

    /**
     * Create a SequenceFile&lt;IntWritable, IntWritable> from int[] data.
     *
     * @param conf
     *            Configuration file
     * @param data
     *            vector data
     * @param baseDirectory
     *            working directory
     * @param filename
     *            name of the file where the matrix is going to be stored.
     * @param start
     *            start index
     * @return Path of the matrix
     * @throws IOException
     */
    public static Path createIntIntFileParent(final Configuration conf, final int[] data,
            final String baseDirectory, final String filename, final int start) throws IOException {

        final String parentUri = baseDirectory + File.separator + filename;
        final String uri = parentUri + File.separator + "data";
        final FileSystem fs = FileSystem.get(URI.create(uri), conf);
        final Path path = new Path(uri);
        final Path parent = new Path(parentUri);

        final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
                IntWritable.class, IntWritable.class);

        try {
            for (int i = 0; i < data.length; i++) {
                writer.append(new IntWritable(i + start), new IntWritable(data[i]));
            }
        } finally {
            IOUtils.closeStream(writer);
        }

        return parent;

    }

    /**
     * Create a SequenceFile&lt;NullWritable, MatrixWritable> from Matrix data.
     *
     * @param conf
     *            Configuration file
     * @param data
     *            Matrix data
     * @param baseDirectory
     *            working directory
     * @param filename
     *            name of the file where the matrix is going to be stored.
     * @return Path of the matrix
     * @throws IOException
     */
    public static Path createMapNullMatrix(final Configuration conf, final Matrix data,
            final String baseDirectory, final String filename) throws IOException {

        final String uri = baseDirectory + File.separator + filename;
        final FileSystem fs = FileSystem.get(URI.create(uri), conf);
        final Path path = new Path(uri);

        final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
                NullWritable.class, MatrixWritable.class);

        try {
            writer.append(NullWritable.get(), new MatrixWritable(data));
        } finally {
            IOUtils.closeStream(writer);
        }

        return path;

    }

}
