/**
 * Copyright 2014 Daniel Valcarce Silva
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

package es.udc.fi.dc.irlab.rmrecommender;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import es.udc.fi.dc.irlab.nmf.clustering.ClusterAssignmentJob;
import es.udc.fi.dc.irlab.nmf.clustering.CountClustersJob;
import es.udc.fi.dc.irlab.nmf.clustering.SubClusterMappingJob;
import es.udc.fi.dc.irlab.nmf.ppc.PPCDriver;
import es.udc.fi.dc.irlab.rm.RM2Job;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Relevance Model Recommender
 *
 */
public class RMRecommenderDriver extends AbstractJob {

    public static final String useCassandraInput = "useCassandraInput";
    public static final String useCassandraOutput = "useCassandraOutput";
    public static final String numberOfUsers = "numberOfUsers";
    public static final String numberOfItems = "numberOfItems";
    public static final String numberOfClusters = "numberOfClusters";
    public static final String usersPerSubCluster = "usersPerSubCluster";
    public static final String numberOfIterations = "numberOfIterations";
    public static final String numberOfRecommendations = "numberOfRecommendations";
    public static final String directory = "directory";
    public static final String cassandraPort = "cassandraPort";
    public static final String cassandraHost = "cassandraHost";
    public static final String cassandraKeyspace = "cassandraKeyspace";
    public static final String cassandraTableIn = "cassandraTableIn";
    public static final String cassandraTableOut = "cassandraTableOut";
    public static final String cassandraPartitioner = "cassandraPartitioner";
    public static final String cassandraTTL = "cassandraTTL";
    public static final String H = "H";
    public static final String W = "W";
    public static final String clustering = "clustering";
    public static final String clusteringCount = "clusteringCount";
    public static final String lambda = "lambda";
    public static final String clusterSplit = "clusterSplit";
    public static final String splitSize = "splitSize";
    public static final String iteration = "iteration";
    public static final String subClustering = "subClustering";
    public static final String normalizationFrequency = "normalizationFrequency";
    public static final String filterUsers = "filterUsers";

    public static final String subClusteringUserPath = "subClusteringUser";
    public static final String subClusteringItemPath = "subClusteringItem";
    public static final String clusteringCountPath = "clusteringCount";
    public static final String mappingPath = "mapping";
    public static final String joinPath = "join";

    private TIntIntMap usersInCluster;
    private TIntIntMap itemsInCluster;

    /**
     * Load default command line arguments.
     */
    private void loadDefaultSetup() {
        addOption(numberOfUsers, "n", "Number of users", true);
        addOption(numberOfItems, "m", "Number of items", true);
        addOption(numberOfClusters, "k", "Number of clusters", true);
        addOption(usersPerSubCluster, "k2", "Number of users per subclusters", String.valueOf(-1));
        addOption(numberOfIterations, "i", "Number of iterations", "10");
        addOption(numberOfRecommendations, "r", "Number of recommendations", "1000");
        addOption(directory, "d", "Working directory", "recommendation");
        addOption(useCassandraInput, "useCas", "Use Cassandra instead of HDFS for input", "false");
        addOption(useCassandraOutput, "useCasOut", "Use Cassandra instead of HDFS for output",
                "false");
        addOption(cassandraPort, "port", "Cassandra TCP port", "9160");
        addOption(cassandraHost, "host", "Cassandra host IP", "127.0.0.1");
        addOption(cassandraKeyspace, "keyspace", "Cassandra keyspace name", "recommendertest");
        addOption(cassandraTableIn, "table", "Cassandra Column Family name for input data",
                "ratings");
        addOption(cassandraTableOut, "table", "Cassandra Column Family name for output data",
                "recommendations");
        addOption(cassandraPartitioner, "partitioner", "Cassandra Partitioner",
                "org.apache.cassandra.dht.Murmur3Partitioner");
        addOption(cassandraTTL, "ttl", "Cassandra TLL for data input", "86400");
        addOption(H, "h", "Initial H matrix", false);
        addOption(W, "w", "Initial W matrix", false);
        addOption(clustering, null, "Clustering results", "clustering");
        addOption(clusteringCount, null, "Clustering count results", "clusteringCount");
        addOption(lambda, null, "Lambda parameter for Jelinek-Mercer smoothing", "0.1");
        addOption(normalizationFrequency, "normFreq", "PCC Normalization Frequency", "12");
        addOption(clusterSplit, "split",
                "Split computation of big clusters (cluster size threshold)", "400");
        addOption(splitSize, "splitSize", "Size of each cluster split", "100");
        addOption(filterUsers, "filter", "Skip users with IDs below given threshold", "0");
    }

    /**
     * Parse command line arguments and update job configuration.
     *
     * @param args
     *            command line arguments
     * @return Configuration
     * @throws IOException
     */
    protected Configuration parseInput(final String[] args) throws IOException {
        /* Add options */
        loadDefaultSetup();

        /* Parse arguments */
        final Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
        if (parsedArgs == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        /* Update job configuration */
        final Configuration conf = getConf();

        for (final Entry<String, List<String>> entry : parsedArgs.entrySet()) {
            final String key = entry.getKey();
            for (final String value : entry.getValue()) {
                conf.set(key.substring(2), value);
            }
        }

        return conf;
    }

    /**
     * Main routine. Launch RMRecommender job.
     *
     * @param args
     *            command line arguments
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RMRecommenderDriver(), args);
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = parseInput(args);

        /* Launch PPC */
        if (conf.getInt(numberOfIterations, -1) > 0) {

            if (ToolRunner.run(conf, new PPCDriver(), args) < 0) {
                throw new RuntimeException("PPCJob failed!");
            }

            /* Set H and W paths properly */
            if (conf.get(RMRecommenderDriver.H) == null) {
                final String directory = conf.get(RMRecommenderDriver.directory);
                conf.set(RMRecommenderDriver.H, directory + File.separator + "H");
                conf.set(RMRecommenderDriver.W, directory + File.separator + "W");
            }

            /* Assign clusters */
            if (ToolRunner.run(conf, new ClusterAssignmentJob(false), args) < 0) {
                throw new RuntimeException("ClusterAssignmentJob failed!");
            }

            /* Launch cluster refinement if required */
            if (conf.getInt(RMRecommenderDriver.usersPerSubCluster, -1) > 0) {
                clusterRefinement(conf, args);
            }

            /* Count number of users per cluster */
            if (ToolRunner.run(conf, new CountClustersJob(), args) < 0) {
                throw new RuntimeException("CountClustersJob failed!");
            }

        }

        /* Run RM2 recommendation algorithm */
        if (conf.getInt(numberOfRecommendations, -1) > 0
                && ToolRunner.run(conf, new RM2Job(), args) < 0) {
            throw new RuntimeException("RMJob failed!");
        }

        return 0;
    }

    /**
     * Do cluster refinement if required
     *
     * @param conf
     *            Job Configuration
     * @param args
     *            Command-line args
     * @throws Exception
     */
    private void clusterRefinement(final Configuration conf, final String[] args) throws Exception {

        int subClusters;
        int count = 0;
        int numberOfUsers;

        final int numberOfClusters = conf.getInt(RMRecommenderDriver.numberOfClusters, -1);
        final int usersPerSubCluster = conf.getInt(RMRecommenderDriver.usersPerSubCluster, 0);

        if (usersPerSubCluster > 0) {

            doMappings(conf, args);

            /* Set configuration for clustering refinement */
            final Configuration jobConf = new Configuration(conf);
            jobConf.set(RMRecommenderDriver.H, "");
            jobConf.set(RMRecommenderDriver.W, "");

            /* For each cluster, find subclusters */
            for (int cluster = 0; cluster < numberOfClusters; cluster++) {
                /* Get number of subclusters */
                numberOfUsers = usersInCluster.get(cluster);
                subClusters = (int) Math.ceil(numberOfUsers / (double) usersPerSubCluster);
                count += subClusters;

                /* Set properties */
                jobConf.setInt(RMRecommenderDriver.subClustering, cluster);
                jobConf.setInt(RMRecommenderDriver.numberOfClusters, subClusters);
                jobConf.setInt(RMRecommenderDriver.numberOfUsers, numberOfUsers);
                jobConf.setInt(RMRecommenderDriver.numberOfItems, itemsInCluster.get(cluster));

                /* Launch PPC in only one cluster */
                if (ToolRunner.run(jobConf, new PPCDriver(), args) < 0) {
                    throw new RuntimeException("ClusterAssignmentJob failed!");
                }

                addPartialH(conf, cluster);

            }

            /* Assign clusters */
            if (ToolRunner.run(conf, new ClusterAssignmentJob(true), args) < 0) {
                throw new RuntimeException("ClusterAssignmentJob failed!");
            }

            conf.setInt(RMRecommenderDriver.numberOfClusters, count);

        }

    }

    /**
     * Add partial H result
     *
     * @param conf
     *            Configuration
     * @param cluster
     *            id of the cluster
     * @throws IOException
     */
    private void addPartialH(final Configuration conf, final int cluster) throws IOException {

        final FileSystem fs = FileSystem.get(conf);
        final String dir = conf.get(RMRecommenderDriver.directory);
        final Path H = new Path(dir + File.separator + "H");
        final Path H_join = new Path(dir + File.separator + RMRecommenderDriver.joinPath
                + File.separator + "cluster" + cluster);

        FileUtil.copy(fs, H, fs, H_join, false, false, conf);

    }

    /**
     * Do the mappings for users and items.
     *
     * @param conf
     *            Job Configuration
     * @param args
     *            Command-line args
     * @throws Exception
     */
    private void doMappings(final Configuration conf, final String[] args) throws Exception {

        final int numberOfClusters = conf.getInt(RMRecommenderDriver.numberOfClusters, -1);

        if (numberOfClusters <= 0) {
            throw new IllegalArgumentException(
                    "Invalid number of clusters (" + numberOfClusters + ")");
        }

        /* Create new IDs for the users and items of each cluster */
        if (ToolRunner.run(conf, new SubClusterMappingJob(), args) < 0) {
            throw new RuntimeException("SubClusterMappingJob failed!");
        }

        /* Read number of users and items per cluster */
        usersInCluster = new TIntIntHashMap(numberOfClusters);
        itemsInCluster = new TIntIntHashMap(numberOfClusters);

        final IntWritable key = new IntWritable();
        final IntWritable val = new IntWritable();

        SequenceFile.Reader[] readers;

        // Users
        final Path clusteringUserCountPath = new Path(conf.get(RMRecommenderDriver.directory)
                + File.separator + RMRecommenderDriver.subClusteringUserPath + File.separator
                + RMRecommenderDriver.clusteringCountPath);
        readers = HadoopUtils.getSequenceReaders(clusteringUserCountPath, conf);

        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                usersInCluster.put(key.get(), val.get());
            }
        }

        // Items
        final Path clusteringItemsCountPath = new Path(conf.get(RMRecommenderDriver.directory)
                + File.separator + RMRecommenderDriver.subClusteringItemPath + File.separator
                + RMRecommenderDriver.clusteringCountPath);
        readers = HadoopUtils.getSequenceReaders(clusteringItemsCountPath, conf);

        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                itemsInCluster.put(key.get(), val.get());
            }
        }

    }

}
