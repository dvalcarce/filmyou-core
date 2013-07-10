package es.udc.fi.dc.irlab.baselinerecommender;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.ToItemPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.cf.taste.hadoop.item.ToUserVectorsReducer;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsMapper;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsReducer;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

public class BaselinePreparePreferenceMatrixJob extends
		PreparePreferenceMatrixJob {

	private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

	protected void loadDefaultSetup() {
		// addInputOption();
		addOutputOption();
		addOption("maxPrefsPerUser", "mppu",
				"max number of preferences to consider per user, "
						+ "users with more preferences will be sampled down");
		addOption("minPrefsPerUser", "mp",
				"ignore users with less preferences than this " + "(default: "
						+ DEFAULT_MIN_PREFS_PER_USER + ')',
				String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
		addOption("ratingShift", "rs", "shift ratings by this value", "0.0");
		addOption("keyspace", "k", "Cassandra Keyspace", true);
		addOption("table", "t", "Cassandra column family", true);
	}

	@Override
	public int run(String[] args) throws Exception {
		loadDefaultSetup();

		Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
		if (parsedArgs == null) {
			return -1;
		}

		int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
		boolean booleanData = Boolean.valueOf(getOption("booleanData"));
		float ratingShift = Float.parseFloat(getOption("ratingShift"));
		String keyspace = getOption("keyspace");
		String table = getOption("table");

		/*
		 * ItemIDIndex job
		 * 
		 * Convert items to an internal index
		 */
		Job itemIDIndex = new Job();
		itemIDIndex.setInputFormatClass(ColumnFamilyInputFormat.class);
		itemIDIndex.setMapperClass(BaselineItemIDIndexMapper.class);
		itemIDIndex.setReducerClass(ItemIDIndexReducer.class);
		itemIDIndex.setJarByClass(ItemIDIndexReducer.class);

		itemIDIndex.setMapOutputKeyClass(VarIntWritable.class);
		itemIDIndex.setMapOutputValueClass(VarLongWritable.class);
		itemIDIndex.setOutputKeyClass(VarIntWritable.class);
		itemIDIndex.setOutputValueClass(VarLongWritable.class);

		itemIDIndex.setOutputFormatClass(SequenceFileOutputFormat.class);

		itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);

		Configuration conf = itemIDIndex.getConfiguration();
		conf.set("mapred.output.dir", getOutputPath(ITEMID_INDEX).toString());

		// Feed all columns for every row in batches of 1000 columns each time
		ConfigHelper.setRangeBatchSize(getConf(), 1000);

		SliceRange sliceRange = new SliceRange(ByteBuffer.wrap(new byte[0]),
				ByteBuffer.wrap(new byte[0]), false, 1000);
		SlicePredicate slice = new SlicePredicate();
		slice.setSlice_range(sliceRange);

		// Cassandra settings
		ConfigHelper.setInputSlicePredicate(conf, slice);
		ConfigHelper.setInputColumnFamily(conf, keyspace, table);
		ConfigHelper.setInputRpcPort(conf, "9160");
		ConfigHelper.setInputInitialAddress(conf, "localhost");
		// ConfigHelper.setInputPartitioner(conf, INPUT_PARTITIONER);
		ConfigHelper.setInputSlicePredicate(conf, slice);

		// Launch job
		itemIDIndex.waitForCompletion(true);

		/*
		 * ToUserVectors job
		 * 
		 * Convert user preferences into a vector per user
		 */
		Job toUserVectors = prepareJob(getInputPath(),
				getOutputPath(USER_VECTORS), TextInputFormat.class,
				ToItemPrefsMapper.class, VarLongWritable.class,
				booleanData ? VarLongWritable.class : EntityPrefWritable.class,
				ToUserVectorsReducer.class, VarLongWritable.class,
				VectorWritable.class, SequenceFileOutputFormat.class);
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
				ToItemVectorsReducer.class, IntWritable.class,
				VectorWritable.class);
		toItemVectors.setCombinerClass(ToItemVectorsReducer.class);

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
}
