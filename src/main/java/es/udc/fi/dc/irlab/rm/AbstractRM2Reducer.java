package es.udc.fi.dc.irlab.rm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

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
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.MapFileOutputFormat;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public abstract class AbstractRM2Reducer<A, B> extends
		Reducer<IntPairWritable, IntDoubleOrPrefWritable, A, B> {

	private Path itemColl;
	private double lambda;
	private int numberOfUsersInCluster;
	private int numberOfUsers;
	private int numberOfItems;
	private Vector clusterUserSum;
	private Matrix preferences;
	private TIntSet clusterItems;
	private TIntObjectMap<TIntSet> userItems;
	private TIntDoubleMap itemCollMap;
	private TIntIntMap clusterCount;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length != 3) {
			throw new FileNotFoundException();
		}

		clusterCount = new TIntIntHashMap(Integer.parseInt(conf
				.get(RMRecommenderDriver.numberOfClusters)));

		SequenceFile.Reader[] readers = HadoopUtils.getLocalSequenceReaders(
				paths[1], conf);

		IntWritable key = new IntWritable();
		IntWritable val = new IntWritable();

		// Read cluster count
		for (SequenceFile.Reader reader : readers) {
			while (reader.next(key, val)) {
				clusterCount.put(key.get(), val.get());
			}
		}

		itemColl = paths[2];
		lambda = Double.valueOf(conf.get(RM2Job.lambdaName));
		numberOfItems = Integer.valueOf(conf
				.get(RMRecommenderDriver.numberOfItems));
		numberOfUsers = Integer.valueOf(conf
				.get(RMRecommenderDriver.numberOfUsers));
	}

	@Override
	protected void reduce(IntPairWritable key,
			Iterable<IntDoubleOrPrefWritable> values, Context context)
			throws IOException, InterruptedException {

		int userId, itemId;
		float score;

		IntDoubleOrPrefWritable entry;
		TIntSet items, unratedItems;
		Iterator<IntDoubleOrPrefWritable> it = values.iterator();

		// Read the number of users in the cluster
		numberOfUsersInCluster = clusterCount.get(key.getFirst());

		// Initialize collections
		clusterUserSum = new RandomAccessSparseVector(numberOfUsers + 1,
				numberOfUsersInCluster);
		preferences = new SparseMatrix(numberOfUsers + 1, numberOfItems + 1);
		clusterItems = new TIntHashSet(numberOfUsersInCluster);
		userItems = new TIntObjectHashMap<TIntSet>(numberOfUsersInCluster);

		// Read the sum of scores of each user
		for (int j = 0; j < numberOfUsersInCluster; j++) {
			entry = it.next();
			userId = entry.getKey();
			clusterUserSum.setQuick(userId, entry.getValue());
			userItems.put(userId, new TIntHashSet());
		}

		context.progress();

		// Read the neighbourhood preferences in order to build preference
		// matrix, item collection set and item users sets
		while (it.hasNext()) {
			entry = it.next();
			userId = entry.getUserId();
			itemId = entry.getItemId();
			score = entry.getScore();
			preferences.set(userId, itemId, score);
			clusterItems.add(itemId);
			userItems.get(userId).add(itemId);
		}

		context.progress();
		buildItemCollMap(context, clusterItems.size());

		System.err.println(">> CLUSTER " + key.getFirst() + " <<");
		System.err.println("# USERS: " + numberOfUsersInCluster + " | "
				+ clusterUserSum.getNumNonZeroElements());
		System.err.println("# ITEMS: " + clusterItems.size());
		System.err.println("# PREFERENCES: "
				+ preferences.getNumNondefaultElements()[0] + " | "
				+ preferences.getNumNondefaultElements()[1]);
		System.err.println();

		// For each user
		for (Vector.Element e : clusterUserSum.nonZeroes()) {
			userId = e.index();
			items = userItems.get(userId);
			unratedItems = new TIntHashSet(clusterItems);
			unratedItems.removeAll(items);
			System.err
					.print("\tCalculating relevance for user " + userId + " ");
			buildRecommendations(context, userId, items, unratedItems,
					key.getFirst());
			System.err.println();
		}

	}

	/**
	 * Build itemCollMap for all items in the cluster.
	 * 
	 * @param context
	 * @param numberOfClusterItems
	 * @throws IOException
	 */
	protected void buildItemCollMap(Context context, int numberOfClusterItems)
			throws IOException {

		final Configuration conf = context.getConfiguration();
		final Reader[] readers = MapFileOutputFormat.getLocalReaders(itemColl,
				conf);
		final Partitioner<IntWritable, DoubleWritable> partitioner = new HashPartitioner<IntWritable, DoubleWritable>();
		final DoubleWritable probability = new DoubleWritable();

		itemCollMap = new TIntDoubleHashMap(numberOfClusterItems);

		clusterItems.forEach(new TIntProcedure() {

			@Override
			public boolean execute(final int item) {
				try {
					MapFileOutputFormat.getEntry(readers, partitioner,
							new IntWritable(item), probability);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				itemCollMap.put(item, probability.get());

				return true;
			}

		});

	}

	/**
	 * Build recommendations using RM2 (conditional sampling) recommender.
	 * 
	 * @param context
	 *            context
	 * @param userId
	 *            user ID
	 * @param items
	 *            items rated by the user
	 * @param unratedItems
	 *            items rated by the cluster, but not by the user
	 * @param cluster
	 *            cluster ID
	 */
	final private void buildRecommendations(final Context context,
			final int userId, final TIntSet items, final TIntSet unratedItems,
			final int cluster) {

		// Calculate relevance for each unrated item
		unratedItems.forEach(new TIntProcedure() {

			/**
			 * This procedure implements RM2
			 */
			@Override
			public boolean execute(final int recommendedItem) {
				int item, neighbour;
				double sum, logResult = 0.0;

				context.progress();
				System.err.print("|");

				// For each rated item
				TIntIterator it = items.iterator();
				while (it.hasNext()) {
					item = it.next();

					sum = 0.0;

					// For each neighbour
					for (Vector.Element e : clusterUserSum.nonZeroes()) {
						neighbour = e.index();
						if (userId == neighbour) {
							continue;
						}

						sum += probItemGivenUser(recommendedItem, neighbour)
								* probItemGivenUser(item, neighbour);
					}
					logResult += Math.log(sum);
				}

				// n = #items rated by the user
				int n = items.size();

				logResult += (n - 1) * Math.log(numberOfItems) - n
						* Math.log(numberOfUsersInCluster);

				try {
					writePreference(context, userId, recommendedItem,
							logResult, cluster);
				} catch (IOException | InterruptedException e) {
					throw new RuntimeException(e);
				}

				return true;

			}

		});

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
		final double rating = preferences.get(user, item);
		final double sum = clusterUserSum.get(user);

		return (1 - lambda) * (rating / sum) + lambda * itemCollMap.get(item);
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
	protected abstract void writePreference(Context context, int userId,
			int itemId, double score, int cluster) throws IOException,
			InterruptedException;

}