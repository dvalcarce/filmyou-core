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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;

/**
 * Integration test class utility
 *
 */
public abstract class HadoopIntegrationTest {

    protected static final double accuracy = 0.0001;

    protected String baseDirectory = "integrationTest";
    protected int cassandraPort;
    protected String cassandraHost;
    protected String cassandraPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    protected String cassandraKeyspace = "recommendertest";
    protected String cassandraTableIn = "ratings";
    protected String cassandraTableOut = "recommendations";
    protected String cassandraTTL = "6000";

    public HadoopIntegrationTest() {
        try {
            cassandraPort = Integer.parseInt(System.getenv("CASSANDRA_PORT"));
        } catch (final NumberFormatException e) {
            cassandraPort = 9160;
        }

        if ((cassandraHost = System.getenv("CASSANDRA_HOST")) == null) {
            cassandraHost = "localhost";
        }
    }

    /**
     * Build basic configuration
     *
     * @return Configuration object
     */
    protected Configuration buildConf() {
        final Configuration conf = new Configuration();

        conf.setInt(RMRecommenderDriver.numberOfRecommendations, 1000);
        conf.set(RMRecommenderDriver.directory, baseDirectory);
        conf.setBoolean(RMRecommenderDriver.useCassandraInput, true);
        conf.setBoolean(RMRecommenderDriver.useCassandraOutput, true);
        conf.setInt(RMRecommenderDriver.cassandraPort, cassandraPort);
        conf.set(RMRecommenderDriver.cassandraHost, cassandraHost);
        conf.set(RMRecommenderDriver.cassandraKeyspace, cassandraKeyspace);
        conf.set(RMRecommenderDriver.cassandraPartitioner, cassandraPartitioner);
        conf.set(RMRecommenderDriver.cassandraTableIn, cassandraTableIn);
        conf.set(RMRecommenderDriver.cassandraTableOut, cassandraTableOut);
        conf.set(RMRecommenderDriver.cassandraTTL, cassandraTTL);
        conf.setFloat(RMRecommenderDriver.lambda, 0.5f);
        conf.setInt(RMRecommenderDriver.clusterSplit, 5);
        conf.setInt(RMRecommenderDriver.splitSize, 3);

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
    protected Configuration buildConf(final Path H, final Path W, final int numberOfUsers,
            final int numberOfItems, final int numberOfClusters, final int numberOfIterations) {

        final Configuration conf = buildConf();

        conf.setInt(RMRecommenderDriver.numberOfUsers, numberOfUsers);
        conf.setInt(RMRecommenderDriver.numberOfItems, numberOfItems);
        conf.setInt(RMRecommenderDriver.numberOfClusters, numberOfClusters);
        conf.setInt(RMRecommenderDriver.numberOfIterations, numberOfIterations);

        if (H != null) {
            conf.set(RMRecommenderDriver.H, H.toString());
        }
        if (W != null) {
            conf.set(RMRecommenderDriver.W, W.toString());
        }

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
     * @param input
     * @param output
     * @return Configuration object
     */
    protected Configuration buildConf(final Path H, final Path W, final int numberOfUsers,
            final int numberOfItems, final int numberOfClusters, final int numberOfIterations,
            final Path input, final Path output) {

        final Configuration conf = buildConf(H, W, numberOfUsers, numberOfItems, numberOfClusters,
                numberOfIterations);

        if (input != null) {
            conf.set(HadoopUtils.inputPathName, input.toString());
        }
        if (output != null) {
            conf.set(HadoopUtils.outputPathName, output.toString());
        }

        return conf;

    }

    /**
     * Build configuration object for Clustering Assignment job.
     *
     * @param H
     *            H matrix path
     * @param clustering
     *            clustering filename
     * @param clusteringCount
     *            clusteringCount filename
     * @param numberOfUsers
     * @param numberOfClusters
     * @return conf
     */
    protected Configuration buildConf(final Path H, final String clustering,
            final String clusteringCount, final int numberOfUsers, final int numberOfClusters) {

        final Configuration conf = buildConf(H, null, numberOfUsers, 0, numberOfClusters, 0);

        if (clustering != null) {
            conf.set(RMRecommenderDriver.clustering, clustering);
        }

        if (clusteringCount != null) {
            conf.set(RMRecommenderDriver.clusteringCount, clusteringCount);
        }

        return conf;

    }

    /**
     * Build configuration object for RM2 Assignment job.
     *
     * @param clustering
     *            clustering filename
     * @param clusteringCount
     *            clusteringCount filename
     * @param numberOfItems
     * @return conf
     */
    protected Configuration buildConf(final String clustering, final String clusteringCount,
            final int numberOfUsers, final int numberOfItems, final int numberOfClusters) {

        final Configuration conf = buildConf(null, null, numberOfUsers, numberOfItems,
                numberOfClusters, 0);

        if (clustering != null) {
            conf.set(RMRecommenderDriver.clustering, clustering);
        }

        if (clusteringCount != null) {
            conf.set(RMRecommenderDriver.clusteringCount, clusteringCount);
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
    protected void compareIntVectorData(final Configuration conf, final double[][] data,
            final String baseDirectory, final Path path) throws IOException {

        final SequenceFile.Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

        final IntWritable key = new IntWritable();
        final VectorWritable val = new VectorWritable();

        // Read cluster count
        int count = 0;
        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                count++;
                for (final Vector.Element element : val.get().all()) {
                    assertEquals(data[key.get() - 1][element.index()], element.get(), accuracy);
                }
            }
        }

        if (count != data.length) {
            fail("Data length does not match (" + count + " vs " + data.length + ")");
        }

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
    protected void compareIntDoubleData(final Configuration conf, final double[] data,
            final String baseDirectory, final Path path) throws IOException {

        final SequenceFile.Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

        final IntWritable key = new IntWritable();
        final DoubleWritable val = new DoubleWritable();

        int count = 0;
        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                count++;
                assertEquals(data[key.get() - 1], val.get(), accuracy);
            }
        }

        if (count != data.length) {
            fail("Data length does not match (" + count + " vs " + data.length + ")");
        }

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
    protected void compareMapIntDoubleData(final Configuration conf, final double[] data,
            final String baseDirectory, final Path path) throws IOException {

        final Reader[] readers = MapFileOutputFormat.getLocalReaders(path, conf);
        final Partitioner<IntWritable, DoubleWritable> partitioner = new HashPartitioner<IntWritable, DoubleWritable>();

        IntWritable key;
        final DoubleWritable val = new DoubleWritable();

        for (int i = 1; i <= data.length; i++) {
            key = new IntWritable(i);

            if (MapFileOutputFormat.getEntry(readers, partitioner, key, val) == null) {
                fail(String.format("data %d not found", i));
            }

            assertEquals(data[i - 1], val.get(), accuracy);
        }

    }

    /**
     * Compare the data vector with the data stored on given path (as a
     * SequenceFile of {@literal <IntWritable, IntWritable>}).
     *
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareIntIntData(final Configuration conf, final int[] data,
            final String baseDirectory, final Path path) throws IOException {

        final SequenceFile.Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

        final IntWritable key = new IntWritable();
        final IntWritable val = new IntWritable();

        int count = 0;
        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                count++;
                assertEquals(data[key.get() - 1], val.get());
            }
        }

        if (count != data.length) {
            fail("Data length does not match (" + count + " vs " + data.length + ")");
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
    protected void compareNullFloatData(final Configuration conf, final double data,
            final String baseDirectory, final Path path) throws IOException {

        final SequenceFile.Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

        final NullWritable key = NullWritable.get();
        final DoubleWritable val = new DoubleWritable();

        for (final SequenceFile.Reader reader : readers) {
            if (reader.next(key, val)) {
                assertEquals(data, val.get(), accuracy);
            } else {
                fail("Data length does not match");
            }
        }

    }

    /**
     * Compare the matrix data with the data stored on given path (as
     * {@literal SequenceFile<IntPairWritable, FloatWritable>}).
     *
     * @param data
     *            data to be compared
     * @param baseDirectory
     *            a temporal file will be created in this folder
     * @param path
     *            path to the data
     * @throws IOException
     */
    protected void compareIntPairFloatData(final Configuration conf, final double[][] data,
            final String baseDirectory, final Path path) throws IOException {

        final SequenceFile.Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

        final Map<Pair<Integer, Integer>, Double> map = new HashMap<Pair<Integer, Integer>, Double>();

        for (int i = 0; i < data.length; i++) {
            map.put(new ImmutablePair<Integer, Integer>((int) data[i][0], (int) data[i][1]),
                    data[i][2]);
        }

        final IntPairWritable key = new IntPairWritable();
        final FloatWritable val = new FloatWritable();

        int item, user, count = 0;

        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                count++;
                item = key.getSecond();
                user = key.getFirst();
                assertEquals(map.get(new ImmutablePair<Integer, Integer>(user, item)), val.get(),
                        accuracy);
            }
        }

        if (count != data.length) {
            fail("Data length does not match (" + count + " and expected " + data.length + ")");
        }

    }

    /**
     * Compare Cassandra data to the given matrix.
     *
     * @param conf
     *            Configuration file
     * @param data
     *            data to be compared
     * @param numberOfUsers
     *            number of users
     * @throws InterruptedException
     */
    protected void compareCassandraData(final Configuration conf, final double[][] data,
            final int numberOfUsers) throws InterruptedException {

        ResultSet result;
        final String keyspace = conf.get("cassandraKeyspace");
        final String table = conf.get("cassandraTableOut");
        final CassandraUtils cassandra = new CassandraUtils(conf.get("cassandraHost"),
                conf.get("cassandraPartitioner"));

        final Map<Pair<Integer, Integer>, Double> map = new HashMap<Pair<Integer, Integer>, Double>();

        for (int i = 0; i < data.length; i++) {
            map.put(new ImmutablePair<Integer, Integer>((int) data[i][0], (int) data[i][1]),
                    data[i][2]);

        }

        int count = 0;
        for (int user = 1; user <= numberOfUsers; user++) {
            result = cassandra.selectData(user, keyspace, table);
            for (final Row row : result) {
                count++;
                assertEquals(
                        map.get(new ImmutablePair<Integer, Integer>(row.getInt(0), row.getInt(1))),
                        row.getFloat(2), accuracy);
            }
        }

        if (count != data.length) {
            fail("Data length does not match (" + count + " vs " + data.length + ")");
        }

        cassandra.shutdownSessions();

    }

}
