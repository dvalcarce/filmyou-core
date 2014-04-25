package es.udc.fi.dc.irlab.rm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntDouble;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.MapFileOutputFormat;
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
public abstract class AbstractRM2Reducer<A, B> extends
		Reducer<IntPairWritable, IntDoubleOrPrefWritable, A, B> {

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

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length != 3) {
			throw new FileNotFoundException();
		}

		int nubmerOfClusters = conf.getInt(
				RMRecommenderDriver.numberOfClusters, -1);
		int numberOfSubClusters = conf.getInt(
				RMRecommenderDriver.numberOfSubClusters, -1);
		if (numberOfSubClusters > 0) {
			nubmerOfClusters *= numberOfSubClusters;
		}

		clusterSizes = new int[nubmerOfClusters];

		SequenceFile.Reader[] readers = HadoopUtils.getLocalSequenceReaders(
				paths[1], conf);

		IntWritable key = new IntWritable();
		IntWritable val = new IntWritable();

		// Read cluster count
		for (SequenceFile.Reader reader : readers) {
			while (reader.next(key, val)) {
				clusterSizes[key.get()] = val.get();
			}
		}

		itemColl = paths[2];
		lambda = Double.valueOf(conf.get(RM2Job.lambdaName));
		numberOfItems = conf.getInt(RMRecommenderDriver.numberOfItems, -1);
		numberOfRecommendations = conf.getInt(
				RMRecommenderDriver.numberOfRecommendations, -1);
	}

	@Override
	protected void reduce(IntPairWritable key,
			Iterable<IntDoubleOrPrefWritable> values, Context context)
			throws IOException, InterruptedException {

		int userId, itemId;
		float score;
		IntDoubleOrPrefWritable entry;

		Iterator<IntDoubleOrPrefWritable> it = values.iterator();

		// Read the number of users in the cluster
		numberOfUsersInCluster = clusterSizes[key.getFirst()];

		// Initialize collections
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

		// Read the neighborhood preferences in order to build preferences
		// matrix
		while (it.hasNext()) {
			entry = it.next();
			userId = entry.getUserId();
			itemId = entry.getItemId();
			score = entry.getScore();

			if (!sparsePreferences.containsKey(itemId)) {
				sparsePreferences.put(itemId, new TIntDoubleHashMap());
			}
			sparsePreferences.get(itemId).put(userId, score);

		}

		createUserAndItemMappings();

		buildItemCollMap(context);

		System.out.println(">> CLUSTER " + key.getFirst() + " <<");
		System.out.println("# USERS: " + numberOfUsersInCluster);
		System.out.println("# ITEMS: " + numberOfItemsInCluster);

		// TEST: Preload p(a|b)
		cache = new double[numberOfUsersInCluster][numberOfItemsInCluster];
		for (int user = 0; user < users.length; user++) {
			for (int item = 0; item < items.length; item++) {
				cache[user][item] = probItemGivenUser(item, user);
			}
		}

		TIntSet unratedItems;
		TIntSet ratedItems;
		TIntSet neighbours;
		// For each user
		for (int user = 0; user < users.length; user++) {
			ratedItems = userItemsMap.get(user);
			unratedItems = new TIntHashSet(itemsSet);
			unratedItems.removeAll(ratedItems);

			neighbours = new TIntHashSet(usersMap.values());
			neighbours.remove(user);

			System.out.print("\tCalculating relevance for user " + user + "\t");
			long startTime = System.nanoTime();
			buildRecommendations(context, user, ratedItems.toArray(),
					unratedItems.toArray(), neighbours.toArray(),
					key.getFirst());
			long estimatedTime = System.nanoTime() - startTime;
			System.out.println((estimatedTime / 1000000000.0) + " seconds");

		}
		System.out.println();

	}

	/**
	 * Create usersMap and items structures
	 */
	private void createUserAndItemMappings() {
		newItemId = 0;
		numberOfItemsInCluster = sparsePreferences.size();
		items = new int[numberOfItemsInCluster];
		itemsSet = new TIntHashSet(numberOfItemsInCluster);

		sparsePreferences
				.forEachEntry(new TIntObjectProcedure<TIntDoubleMap>() {

					/* For each item */
					@Override
					public boolean execute(final int itemId,
							final TIntDoubleMap value) {
						items[newItemId] = itemId;
						itemsSet.add(newItemId);

						value.forEachEntry(new TIntDoubleProcedure() {

							/* For each user */
							@Override
							public boolean execute(final int userId,
									final double score) {

								userItemsMap.get(usersMap.get(userId)).add(
										newItemId);
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
	protected void buildItemCollMap(Context context) throws IOException {

		final Configuration conf = context.getConfiguration();
		final Reader[] readers = MapFileOutputFormat.getLocalReaders(itemColl,
				conf);
		final Partitioner<IntWritable, DoubleWritable> partitioner = new HashPartitioner<IntWritable, DoubleWritable>();
		final DoubleWritable probability = new DoubleWritable();

		itemProbInColl = new double[numberOfItemsInCluster];

		for (int i = 0; i < items.length; i++) {
			try {
				MapFileOutputFormat.getEntry(readers, partitioner,
						new IntWritable(items[i]), probability);
			} catch (IOException e) {
				throw new RuntimeException(e);
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
	private void buildRecommendations(final Context context, final int userId,
			final int[] ratedItems, final int[] unratedItems,
			final int[] neighbours, final int cluster) {

		SortedSet<IntDouble> prefs = new TreeSet<IntDouble>();

		/* Calculate relevance for each unrated item */
		for (int recommendedItem : unratedItems) {

			double logResult = 0.0;

			/* For each rated item */
			for (int item : ratedItems) {

				double sum = 0.0;

				/* For each neighbour */
				for (int neighbour : neighbours) {

					sum += cache[neighbour][recommendedItem]
							* cache[neighbour][item];

				}

				logResult += Math.log(sum);

			}

			// n = #items rated by the user
			int n = ratedItems.length;

			logResult += (n - 1) * Math.log(numberOfItems) - n
					* Math.log(numberOfUsersInCluster);

			prefs.add(new IntDouble(recommendedItem, logResult));
			context.progress();

		}

		/* Write top recommendations for the given user */
		Iterator<IntDouble> it = prefs.iterator();
		IntDouble element;
		int iterations = Math.min(numberOfRecommendations, prefs.size());

		for (int i = 0; i < iterations; i++) {
			element = it.next();
			try {
				writePreference(context, users[userId],
						items[element.getKey()], element.getValue(), cluster);
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
	private double probItemGivenUser(final int item, final int user) {
		final double rating = sparsePreferences.get(items[item]).get(
				users[user]);
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
	protected abstract void writePreference(final Context context,
			final int userId, final int itemId, final double score,
			final int cluster) throws IOException, InterruptedException;

}
