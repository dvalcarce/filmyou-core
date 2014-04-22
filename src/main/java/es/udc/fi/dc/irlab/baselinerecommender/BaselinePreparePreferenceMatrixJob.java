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

package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.cf.taste.hadoop.item.ToUserVectorsReducer;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsMapper;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

/**
 * Launches the first jobs of the baseline recommendation algorithm.
 * 
 */
public class BaselinePreparePreferenceMatrixJob extends
		PreparePreferenceMatrixJob {
	private static final int MINIMUM_ITEM_ID = 1;
	private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

	public static void main(String[] args) throws Exception {
		if (ToolRunner.run(new BaselinePreparePreferenceMatrixJob(), args) < 0) {
			throw new RuntimeException(
					"BaselinePreparePreferenceMatrixJob failed!");
		}
	}

	int minPrefsPerUser;
	boolean booleanData;
	float ratingShift;
	String keyspace;

	String table;

	/**
	 * Convert user preferences into a vector per user.
	 * 
	 * @return int succeed
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	protected int createUserVectors() throws IOException,
			ClassNotFoundException, InterruptedException {

		Job toUserVectors = new Job(new Configuration(getConf()),
				"toUserVectors");

		toUserVectors.setMapperClass(BaselineToItemPrefsMapper.class);
		toUserVectors.setReducerClass(ToUserVectorsReducer.class);
		toUserVectors.setJarByClass(BaselinePreparePreferenceMatrixJob.class);

		toUserVectors.setMapOutputKeyClass(VarLongWritable.class);
		toUserVectors
				.setMapOutputValueClass(booleanData ? VarLongWritable.class
						: EntityPrefWritable.class);
		toUserVectors.setOutputKeyClass(VarLongWritable.class);
		toUserVectors.setOutputValueClass(VectorWritable.class);

		toUserVectors.setInputFormatClass(CqlPagingInputFormat.class);
		toUserVectors.setOutputFormatClass(SequenceFileOutputFormat.class);

		toUserVectors.setNumReduceTasks(5);

		Configuration conf = toUserVectors.getConfiguration();
		conf.set("mapred.output.dir", getOutputPath(USER_VECTORS).toString());

		// Cassandra settings
		String port = "9160";
		String host = "127.0.0.1";
		ConfigHelper.setInputRpcPort(conf, port);
		ConfigHelper.setInputInitialAddress(conf, host);
		ConfigHelper.setInputPartitioner(conf,
				"org.apache.cassandra.dht.Murmur3Partitioner");
		ConfigHelper.setInputColumnFamily(conf, keyspace, table, true);

		toUserVectors.getConfiguration().setBoolean(
				RecommenderJob.BOOLEAN_DATA, booleanData);
		toUserVectors.getConfiguration().setInt(
				ToUserVectorsReducer.MIN_PREFERENCES_PER_USER, minPrefsPerUser);
		toUserVectors.getConfiguration().set(ToEntityPrefsMapper.RATING_SHIFT,
				String.valueOf(ratingShift));

		boolean succeeded = toUserVectors.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}

		// we need the number of users later
		int numberOfUsers = (int) toUserVectors.getCounters()
				.findCounter(ToUserVectorsReducer.Counters.USERS).getValue();
		HadoopUtil.writeInt(numberOfUsers, getOutputPath(NUM_USERS), getConf());

		// build the rating matrix
		Job toItemVectors = prepareJob(getOutputPath(USER_VECTORS),
				getOutputPath(RATING_MATRIX), ToItemVectorsMapper.class,
				IntWritable.class, VectorWritable.class,
				BaselineToItemVectorsReducer.class, IntWritable.class,
				VectorWritable.class);
		toItemVectors.setCombinerClass(BaselineToItemVectorsReducer.class);

		/* configure sampling regarding the uservectors */
		if (hasOption("maxPrefsPerUser")) {
			int samplingSize = Integer.parseInt(getOption("maxPrefsPerUser"));
			toItemVectors.getConfiguration().setInt(
					ToItemVectorsMapper.SAMPLE_SIZE, samplingSize);
		}

		succeeded = toItemVectors.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		return 0;
	}

	/**
	 * ItemIDIndex job. Gets the minimum itemID.
	 * 
	 * @return long minimum itemID
	 */
	protected long getMinimum() {
		return MINIMUM_ITEM_ID;
	}

	/**
	 * ItemIDIndex job. Gets the minimum itemID using MapReduce.
	 * 
	 * @return int succeed
	 */
	protected int getMinimumMR() throws ClassNotFoundException, IOException,
			InterruptedException {

		Job itemIDIndex = new Job(new Configuration(getConf()), "itemIDIndex");

		itemIDIndex.setMapperClass(BaselineItemIDIndexMapper.class);
		itemIDIndex.setReducerClass(ItemIDIndexReducer.class);
		itemIDIndex.setJarByClass(BaselinePreparePreferenceMatrixJob.class);

		itemIDIndex.setMapOutputKeyClass(VarIntWritable.class);
		itemIDIndex.setMapOutputValueClass(VarLongWritable.class);
		itemIDIndex.setOutputKeyClass(VarIntWritable.class);
		itemIDIndex.setOutputValueClass(VarLongWritable.class);

		itemIDIndex.setInputFormatClass(CqlPagingInputFormat.class);
		itemIDIndex.setOutputFormatClass(SequenceFileOutputFormat.class);

		itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);

		itemIDIndex.setNumReduceTasks(5);

		Configuration conf = itemIDIndex.getConfiguration();
		conf.set("mapred.output.dir", getOutputPath(ITEMID_INDEX).toString());

		// Cassandra settings
		String port = "9160";
		String host = "127.0.0.1";
		ConfigHelper.setInputRpcPort(conf, port);
		ConfigHelper.setInputInitialAddress(conf, host);
		ConfigHelper.setInputPartitioner(conf,
				"org.apache.cassandra.dht.Murmur3Partitioner");
		ConfigHelper.setInputColumnFamily(conf, keyspace, table, true);

		boolean succeeded = itemIDIndex.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		return 0;
	}

	/**
	 * Load default command line arguments.
	 */
	protected void loadDefaultSetup() {
		addOutputOption();
		addOption("maxPrefsPerUser", "mppu",
				"max number of preferences to consider per user, "
						+ "users with more preferences will be sampled down");
		addOption("minPrefsPerUser", "mp",
				"ignore users with less preferences than this " + "(default: "
						+ DEFAULT_MIN_PREFS_PER_USER + ')',
				String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
		addOption("ratingShift", "rs", "shift ratings by this value", "0.0");
		addOption("booleanData", "b", "Treat input as without pref values",
				Boolean.FALSE.toString());
		addOption("keyspace", "k", "Cassandra Keyspace", true);
		addOption("table", "t", "Cassandra column family", true);
	}

	/**
	 * Run all jobs related to preparing the preference matrix.
	 */
	@Override
	public int run(String[] args) throws Exception {

		loadDefaultSetup();

		Map<String, List<String>> parsedArgs = parseArguments(args, true, false);
		if (parsedArgs == null) {
			return -1;
		}

		minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
		booleanData = Boolean.valueOf(getOption("booleanData"));
		ratingShift = Float.parseFloat(getOption("ratingShift"));
		keyspace = getOption("keyspace");
		table = getOption("table");

		if (getMinimum() < 0) {
			return -1;
		}

		if (createUserVectors() < 0) {
			return -1;
		}

		return 0;
	}
}
