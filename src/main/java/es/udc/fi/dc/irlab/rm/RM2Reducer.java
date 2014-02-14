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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit &lt;j, i, A_{i,j}> to Cassandra from &lt;k, {|k|} U {(j, sum_i A_{i,j})}
 * U {(i, j, A_{i,j})}>.
 */
public class RM2Reducer
	extends
	Reducer<IntPairWritable, IntDoubleOrPrefWritable, NullWritable, VectorWritable> {

    private Path itemColl;

    private double lambda;

    private int numberOfUsersInCluster;
    private int numberOfUsers;
    private int numberOfItems;

    private Vector userSum;

    private Matrix preferences;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

	if (localFiles.length != 2) {
	    throw new FileNotFoundException(getClass()
		    + ": Missing distributed cache files.");
	}

	itemColl = localFiles[1];
	lambda = Double.valueOf(conf.get(RM2Job.lambdaName));
	numberOfItems = Integer.valueOf(conf.get("numberOfItems"));
	numberOfUsers = Integer.valueOf(conf.get("numberOfUsers"));
    }

    @Override
    protected void reduce(IntPairWritable key,
	    Iterable<IntDoubleOrPrefWritable> values, Context context)
	    throws IOException, InterruptedException {

	IntDoubleOrPrefWritable entry;
	Iterator<IntDoubleOrPrefWritable> it = values.iterator();

	// Read the number of users in the cluster
	entry = it.next();
	numberOfUsersInCluster = entry.getKey();

	// Read the sum of scores of each user
	userSum = new RandomAccessSparseVector(numberOfUsers,
		numberOfUsersInCluster);
	for (int j = 0; j < numberOfUsersInCluster; j++) {
	    entry = it.next();
	    userSum.setQuick(entry.getKey(), entry.getValue());
	}

	// Read the neighbours preferences
	preferences = new SparseMatrix(numberOfUsersInCluster, numberOfItems);
	while (it.hasNext()) {
	    entry = it.next();
	    preferences.setQuick(entry.getUserID(), entry.getItemID(),
		    entry.getScore());

	}

	context.write(NullWritable.get(), new VectorWritable(userSum));

    }

    @SuppressWarnings("unused")
    private void writePreference(Context context) throws IOException,
	    InterruptedException {

	int userID = 0;
	int itemID = 0;
	float score = 0.0f;

	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
	keys.put("user", ByteBufferUtil.bytes(userID));
	keys.put("movie", ByteBufferUtil.bytes(itemID));

	List<ByteBuffer> value = new LinkedList<ByteBuffer>();
	value.add(ByteBufferUtil.bytes(score));

	// context.write(keys, value);

    }

}
