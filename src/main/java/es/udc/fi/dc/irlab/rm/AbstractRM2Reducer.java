package es.udc.fi.dc.irlab.rm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntDouble;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.IntKeyPartitioner;
import es.udc.fi.dc.irlab.util.MapFileOutputFormat;
import es.udc.fi.dc.irlab.util.StringIntPairWritable;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntDoubleProcedure;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements RM2-based collaborative filtering algorithm
 *
 * @param <A>
 *            Output key
 * @param <B>
 *            Output value
 */
public abstract class AbstractRM2Reducer<A, B>
        extends Reducer<StringIntPairWritable, IntDoubleOrPrefWritable, A, B> {

    private static final Log LOG = LogFactory.getLog(AbstractRM2Reducer.class);

    private Path itemColl;
    private double lambda;
    private int numberOfUsersInCluster;
    private int numberOfItemsInCluster;
    private int numberOfItems;
    private int numberOfRecommendations;

    private TIntObjectMap<TIntDoubleMap> sparsePreferences;
    private TIntObjectMap<TIntSet> userItemsMap;

    private TIntIntMap usersMap;
    private TIntSet itemsSet;

    private int[] clusterSizes;
    private int[] users;
    private int[] items;

    private double[] userSums;
    private double[] itemProbInColl;

    private double[][] cache;

    private int newItemId;

    private int split;
    private int numberOfSplits;

    @Override
    public void setup(final Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        final Path[] paths = DistributedCache.getLocalCacheFiles(conf);

        if (paths == null || paths.length != 3) {
            throw new FileNotFoundException();
        }

        final int nubmerOfClusters = conf.getInt(RMRecommenderDriver.numberOfClusters, -1);

        clusterSizes = new int[nubmerOfClusters];

        final SequenceFile.Reader[] readers = HadoopUtils.getLocalSequenceReaders(paths[1], conf);

        final IntWritable key = new IntWritable();
        final IntWritable val = new IntWritable();

        // Read cluster count
        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                clusterSizes[key.get()] = val.get();
            }
        }

        itemColl = paths[2];
        lambda = Double.valueOf(conf.get(RM2Job.LAMBDA_NAME));
        numberOfItems = conf.getInt(RMRecommenderDriver.numberOfItems, -1);
        numberOfRecommendations = conf.getInt(RMRecommenderDriver.numberOfRecommendations, -1);
    }

    final private int parseCluster(final String str) {
        if (!str.contains("-")) {
            final int cluster = Integer.valueOf(str);
            numberOfSplits = 1;
            split = 0;
            return cluster;
        }

        final Pattern pattern = Pattern.compile("([0-9]+)-([0-9]+)-([0-9]+)");
        final Matcher matcher = pattern.matcher(str);
        matcher.find();
        split = Integer.valueOf(matcher.group(2));
        numberOfSplits = Integer.valueOf(matcher.group(3));
        return Integer.valueOf(matcher.group(1));
    }

    @Override
    protected void reduce(final StringIntPairWritable key,
            final Iterable<IntDoubleOrPrefWritable> values, final Context context)
                    throws IOException, InterruptedException {

        int userId, itemId;
        float score;
        IntDoubleOrPrefWritable entry;
        final String cluster = key.getKey();
        final int thisCluster = parseCluster(cluster);

        final Iterator<IntDoubleOrPrefWritable> it = values.iterator();

        // Read the number of users in the cluster
        numberOfUsersInCluster = clusterSizes[thisCluster];

        // Initialise collections
        sparsePreferences = new TIntObjectHashMap<TIntDoubleMap>();
        userItemsMap = new TIntObjectHashMap<TIntSet>(numberOfUsersInCluster);

        // Read the sum of scores of each user
        usersMap = new TIntIntHashMap(numberOfUsersInCluster);
        userSums = new double[numberOfUsersInCluster];
        users = new int[numberOfUsersInCluster];
        for (int j = 0; j < numberOfUsersInCluster; j++) {
            entry = it.next();
            userId = entry.getKey();
            users[j] = userId;
            usersMap.put(userId, j);
            userItemsMap.put(j, new TIntHashSet());
            userSums[j] = entry.getValue();
        }

        // Read the neighbourhood preferences in order to build preferences
        // matrix
        while (it.hasNext()) {
            entry = it.next();
            userId = entry.getUserId();
            itemId = entry.getItemId();
            score = entry.getScore();

            if (!sparsePreferences.containsKey(itemId)) {
                sparsePreferences.put(itemId, new TIntDoubleHashMap());
            }
            sparsePreferences.get(itemId).put(userId, (double) score);
        }

        createUserAndItemMappings();

        LOG.info("Cluster " + thisCluster + "-" + split + ": " + users.length + "\tusers ("
                + numberOfUsersInCluster + ") and " + items.length + "\titems ("
                + numberOfItemsInCluster + ")");

        buildItemCollMap(context);

        // Preload p(a|b)
        cache = new double[numberOfUsersInCluster][numberOfItemsInCluster];
        for (int userID = 0; userID < numberOfUsersInCluster; userID++) {
            for (int itemID = 0; itemID < numberOfItemsInCluster; itemID++) {
                cache[userID][itemID] = probItemGivenUser(itemID, userID);
            }
        }

        TIntSet unratedItems;
        TIntSet ratedItems;
        TIntSet neighbours;

        final Configuration conf = context.getConfiguration();
        final int userFilter = conf.getInt(RMRecommenderDriver.filterUsers, 0);

        long time = System.nanoTime();

        // For each user
        for (int userID = 0; userID < numberOfUsersInCluster; userID++) {
            if (users[userID] % numberOfSplits != split) {
                continue;
            }
            ratedItems = userItemsMap.get(userID);
            unratedItems = new TIntHashSet(itemsSet);
            unratedItems.removeAll(ratedItems);

            if (unratedItems.size() == 0) {
                LOG.warn("User " + users[userID] + "does not have any unrated item in the cluster");
                continue;
            }

            neighbours = new TIntHashSet(usersMap.values());
            neighbours.remove(userID);

            context.progress();

            // Skip users
            if (users[userID] < userFilter) {
                continue;
            }
            buildRecommendations(context, userID, ratedItems.toArray(), unratedItems.toArray(),
                    neighbours.toArray(), thisCluster);
        }

        time = System.nanoTime() - time;

        LOG.info("Cluster " + thisCluster + "-" + split + ": " + (time / 1000000000.0)
                + "\t seconds");

    }

    /**
     * Create usersMap and items structures
     */
    final private void createUserAndItemMappings() {
        newItemId = 0;
        numberOfItemsInCluster = sparsePreferences.size();
        items = new int[numberOfItemsInCluster];
        itemsSet = new TIntHashSet(numberOfItemsInCluster);

        sparsePreferences.forEachEntry(new TIntObjectProcedure<TIntDoubleMap>() {

            /* For each item */
            @Override
            public boolean execute(final int itemId, final TIntDoubleMap value) {
                items[newItemId] = itemId;
                itemsSet.add(newItemId);

                value.forEachEntry(new TIntDoubleProcedure() {

                    /* For each user */
                    @Override
                    public boolean execute(final int userId, final double score) {

                        userItemsMap.get(usersMap.get(userId)).add(newItemId);
                        return true;

                    }

                });

                newItemId++;
                return true;

            }

        });

    }

    /**
     * Build itemCollMap for all items in the cluster.
     *
     * @param context
     * @param numberOfClusterItems
     * @throws IOException
     */
    final private void buildItemCollMap(final Context context) throws IOException {

        final Configuration conf = context.getConfiguration();
        final Reader[] readers = MapFileOutputFormat.getLocalReaders(itemColl, conf);
        final Partitioner<IntWritable, Writable> partitioner = new IntKeyPartitioner();
        final DoubleWritable probability = new DoubleWritable();

        itemProbInColl = new double[numberOfItemsInCluster];

        Writable entry;
        for (int i = 0; i < items.length; i++) {
            entry = MapFileOutputFormat.getEntry(readers, partitioner, new IntWritable(items[i]),
                    probability);
            if (entry == null) {
                throw new RuntimeException(
                        "p(" + items[i] + "|C) not found | partition = " + partitioner.getPartition(
                                new IntWritable(items[i]), probability, readers.length));
            }

            itemProbInColl[i] = probability.get();
        }

    }

    /**
     * Build recommendations using RM2 (conditional sampling) recommender.
     *
     * @param context
     *            context
     * @param userId
     *            user ID
     * @param ratedItems
     *            items rated by the user
     * @param unratedItems
     *            items rated by the cluster, but not by the user
     * @param neighbours
     *            user neighborhood
     * @param cluster
     *            cluster ID
     */
    final private void buildRecommendations(final Context context, final int userId,
            final int[] ratedItems, final int[] unratedItems, final int[] neighbours,
            final int cluster) {

        final PriorityQueue<IntDouble> prefs = new PriorityQueue<IntDouble>(unratedItems.length);

        final int n = ratedItems.length;
        final double pvpi = (n - 1) * Math.log(numberOfItems)
                - n * Math.log(numberOfUsersInCluster);

        /* Calculate relevance for each unrated item */
        for (final int recommendedItem : unratedItems) {

            double logResult = 0.0;

            /* For each rated item */
            for (final int item : ratedItems) {

                double sum = 0.0;

                /* For each neighbour */
                for (final int neighbour : neighbours) {

                    sum += cache[neighbour][recommendedItem] * cache[neighbour][item];

                }

                logResult += Math.log(sum);

            }

            logResult += pvpi;

            prefs.add(new IntDouble(recommendedItem, logResult));

        }

        /* Write top recommendations for the given user */
        IntDouble element;
        final int iterations = Math.min(numberOfRecommendations, prefs.size());
        for (int i = 0; i < iterations; i++) {
            element = prefs.poll();
            try {
                writePreference(context, users[userId], items[element.getKey()], element.getValue(),
                        cluster);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * Estimate the probability of an item given a user computed by smoothing
     * the maximum likelihood estimate with the probability in the collection
     * using Jelinek-Mercer smoothing.
     *
     * @param item
     *            itemId
     * @param user
     *            userId
     * @return
     */
    final private double probItemGivenUser(final int item, final int user) {
        final double rating = sparsePreferences.get(items[item]).get(users[user]);
        final double sum = userSums[user];

        return (1 - lambda) * (rating / sum) + lambda * itemProbInColl[item];
    }

    /**
     * Write given preference to persistence.
     *
     * @param context
     *            reduce context
     * @param userId
     *            user ID
     * @param itemId
     *            item ID
     * @param score
     *            predicted score
     * @throws IOException
     * @throws InterruptedException
     */
    protected abstract void writePreference(final Context context, final int userId,
            final int itemId, final double score, final int cluster)
                    throws IOException, InterruptedException;

}
