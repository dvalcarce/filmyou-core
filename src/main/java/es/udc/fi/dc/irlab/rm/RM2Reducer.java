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

package es.udc.fi.dc.irlab.rm;

import es.udc.fi.dc.irlab.util.MapFileOutputFormat;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;

/**
 * Emit &lt;j, i, A_{i,j}> to Cassandra from &lt;k, {|k|} U {(j, sum_i A_{i,j})}
 * U {(i, j, A_{i,j})}>.
 */
public class RM2Reducer
	extends
	Reducer<IntPairWritable, IntDoubleOrPrefWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

    private Path itemColl;

    private double lambda;

    private int numberOfUsersInCluster;
    private int numberOfUsers;
    private int numberOfItems;

    private Vector clusterUserSum;
    private Matrix preferences;
    private TIntSet clusterItems;
    private TIntSet clusterUsers;
    private TIntObjectMap<TIntSet> userItems;
    private TIntDoubleMap itemCollMap;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();

	itemColl = new Path(conf.get(RM2Job.itemCollName));
	lambda = Double.valueOf(conf.get(RM2Job.lambdaName));
	numberOfItems = Integer.valueOf(conf.get("numberOfItems"));
	numberOfUsers = Integer.valueOf(conf.get("numberOfUsers"));
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
	entry = it.next();
	numberOfUsersInCluster = entry.getKey();

	// Initialize collections
	clusterUserSum = new RandomAccessSparseVector(numberOfUsers,
		numberOfUsersInCluster);
	preferences = new SparseMatrix(numberOfUsers, numberOfItems);
	clusterUsers = new TIntHashSet(numberOfUsersInCluster);
	clusterItems = new TIntHashSet(numberOfUsersInCluster);
	userItems = new TIntObjectHashMap<TIntSet>(numberOfUsersInCluster);

	// Read the sum of scores of each user
	for (int j = 0; j < numberOfUsersInCluster; j++) {
	    entry = it.next();
	    userId = entry.getKey() - 1;
	    clusterUserSum.setQuick(userId, entry.getValue());
	    userItems.put(userId, new TIntHashSet());
	    clusterUsers.add(userId);
	}

	// Read the neighbourhood preferences in order to build preference
	// matrix, item collection set and item users sets
	while (it.hasNext()) {
	    entry = it.next();
	    userId = entry.getUserID() - 1;
	    itemId = entry.getItemID() - 1;
	    score = entry.getScore();

	    preferences.setQuick(userId, itemId, score);
	    clusterItems.add(itemId);
	    userItems.get(userId).add(itemId);
	}

	buildItemCollMap(context, clusterItems.size());

	// For each user
	for (Vector.Element e : clusterUserSum.nonZeroes()) {
	    userId = e.index();
	    items = userItems.get(userId);
	    unratedItems = new TIntHashSet(clusterItems);
	    unratedItems.removeAll(items);
	    buildRecommendations(context, userId, items, unratedItems,
		    key.getFirst());
	}
    }

    /**
     * Build itemCollMap for all items in the cluster.
     * 
     * @param context
     * @param numberOfClusterItems
     * @throws IOException
     */
    private void buildItemCollMap(Context context, int numberOfClusterItems)
	    throws IOException {
	Configuration conf = context.getConfiguration();
	final Reader[] readers = MapFileOutputFormat.getReaders(itemColl, conf);
	final Partitioner<IntWritable, DoubleWritable> partitioner = new HashPartitioner<IntWritable, DoubleWritable>();
	final DoubleWritable probability = new DoubleWritable();

	itemCollMap = new TIntDoubleHashMap(numberOfClusterItems);

	clusterItems.forEach(new TIntProcedure() {

	    @Override
	    public boolean execute(int item) {
		try {
		    MapFileOutputFormat.getEntry(readers, partitioner,
			    new IntWritable(item + 1), probability);
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
    private void buildRecommendations(final Context context, final int userId,
	    final TIntSet items, final TIntSet unratedItems, final int cluster) {

	// Calculate relevance for each unrated item
	unratedItems.forEach(new TIntProcedure() {

	    /**
	     * This procedure implements RM2
	     */
	    @Override
	    public boolean execute(int recommendedItem) {
		int item, neighbour;
		double sum, logResult = 0.0;

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

		// n = #items rated by user
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
    private double probItemGivenUser(int item, int user) {
	double rating = preferences.get(user, item);
	double sum = clusterUserSum.get(user);

	return (1 - lambda) * (rating / sum) + lambda * itemCollMap.get(item);
    }

    /**
     * Write preference to Cassandra.
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
    private void writePreference(Context context, int userId, int itemId,
	    double score, int cluster) throws IOException, InterruptedException {

	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
	keys.put("user", ByteBufferUtil.bytes(userId + 1));
	keys.put("movie", ByteBufferUtil.bytes(itemId + 1));
	keys.put("relevance", ByteBufferUtil.bytes((float) score));

	List<ByteBuffer> value = new LinkedList<ByteBuffer>();
	value.add(ByteBufferUtil.bytes(cluster));

	context.write(keys, value);

    }

}