package es.udc.fi.dc.irlab.baselinerecommender;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CooccurrenceCountSimilarity;

public class BaselineRecommenderJob extends AbstractJob {

	public static final String BOOLEAN_DATA = "booleanData";
	private static final int DEFAULT_NUM_RECOMMENDATIONS = 100;
	private static final int DEFAULT_MAX_SIMILARITIES_PER_ITEM = 100;
	private static final int DEFAULT_MAX_PREFS_PER_USER = 1000;
	private static final int DEFAULT_MIN_PREFS_PER_USER = 1;
	private static final int DEFAULT_MAX_PREFS_PER_USER_CONSIDERED = 50;
	private static final String KEYSPACE = "demo";
	private static final String TABLE = "ratings";

	/**
	 * 
	 */
	protected void loadDefaultSetup() {
		// addInputOption();
		addOutputOption();
		addOption("numRecommendations", "n",
				"Number of recommendations per user",
				String.valueOf(DEFAULT_NUM_RECOMMENDATIONS));
		// addOption("usersFile", null, "File of users to recommend for", null);
		// addOption("itemsFile", null, "File of items to recommend for", null);
		// addOption(
		// "filterFile",
		// "f",
		// "File containing comma-separated userID,itemID pairs. Used to exclude the item from "
		// + "the recommendations for that user (optional)", null);
		// addOption("booleanData", "b", "Treat input as without pref values",
		// Boolean.FALSE.toString());
		addOption(
				"maxPrefsPerUser",
				"mxp",
				"Maximum number of preferences considered per user in final recommendation phase",
				String.valueOf(DEFAULT_MAX_PREFS_PER_USER_CONSIDERED));
		addOption("minPrefsPerUser", "mp",
				"ignore users with less preferences than this in the similarity computation "
						+ "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')',
				String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
		addOption("maxSimilaritiesPerItem", "m",
				"Maximum number of similarities considered per item ",
				String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ITEM));
		addOption(
				"maxPrefsPerUserInItemSimilarity",
				"mppuiis",
				"max number of preferences to consider per user in the "
						+ "item similarity computation phase, users with more preferences will be sampled down (default: "
						+ DEFAULT_MAX_PREFS_PER_USER + ')',
				String.valueOf(DEFAULT_MAX_PREFS_PER_USER));
		addOption("similarityClassname", "s",
				"Name of distributed similarity measures class to instantiate",
				String.valueOf(CooccurrenceCountSimilarity.class));
		addOption("threshold", "tr",
				"discard item pairs with a similarity value below this", false);
	}

	/**
	 * 
	 */
	public int run(String[] args) throws Exception {
		loadDefaultSetup();

		Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
		if (parsedArgs == null) {
			return -1;
		}

		Path outputPath = getOutputPath();
		int numRecommendations = Integer
				.parseInt(getOption("numRecommendations"));
		int maxPrefsPerUser = Integer.parseInt(getOption("maxPrefsPerUser"));
		int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
		int maxPrefsPerUserInItemSimilarity = Integer
				.parseInt(getOption("maxPrefsPerUserInItemSimilarity"));
		int maxSimilaritiesPerItem = Integer
				.parseInt(getOption("maxSimilaritiesPerItem"));
		String similarityClassname = getOption("similarityClassname");
		double threshold = hasOption("threshold") ? Double
				.parseDouble(getOption("threshold"))
				: RowSimilarityJob.NO_THRESHOLD;

		Path prepPath = getTempPath("preparePreferenceMatrix");
		Path similarityMatrixPath = getTempPath("similarityMatrix");
		Path prePartialMultiplyPath1 = getTempPath("prePartialMultiply1");
		Path prePartialMultiplyPath2 = getTempPath("prePartialMultiply2");
		Path explicitFilterPath = getTempPath("explicitFilterPath");
		Path partialMultiplyPath = getTempPath("partialMultiply");

		/********************************/

		// AtomicInteger currentPhase = new AtomicInteger();

		// ByteBuffer noValue = ByteBuffer.wrap(new byte[0]);
		// if (shouldRunNextPhase(parsedArgs, currentPhase)) {
		// Job itemIDIndex = prepareJob(inputPath, itemIDIndexPath,
		// ColumnFamilyInputFormat.class,
		// BaselineItemIDIndexMapper.class, VarIntWritable.class,
		// VarLongWritable.class, ItemIDIndexReducer.class,
		// VarIntWritable.class, VarLongWritable.class,
		// SequenceFileOutputFormat.class);
		// SlicePredicate slice = new SlicePredicate();
		// slice.setSlice_range(new SliceRange(noValue, noValue, false,
		// Integer.MAX_VALUE));
		// Configuration conf = itemIDIndex.getConfiguration();
		// ConfigHelper.setInputInitialAddress(conf, "localhost");
		// ConfigHelper.setInputRpcPort(conf, "9160");
		// ConfigHelper.setInputSlicePredicate(conf, slice);
		// ConfigHelper.setInputColumnFamily(conf, "recommender", "users");
		// itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);
		// itemIDIndex.waitForCompletion(true);
		// }
		//
		// int numberOfUsers = 0;
		// if (shouldRunNextPhase(parsedArgs, currentPhase)) {
		// Job toUserVector = prepareJob(inputPath, userVectorPath,
		// ColumnFamilyInputFormat.class, ToItemPrefsMapper.class,
		// VarLongWritable.class, booleanData ? VarLongWritable.class
		// : EntityPrefWritable.class,
		// ToUserVectorsReducer.class, VarLongWritable.class,
		// VectorWritable.class, SequenceFileOutputFormat.class);
		// SlicePredicate slice = new SlicePredicate();
		// slice.setSlice_range(new SliceRange(noValue, noValue, false,
		// Integer.MAX_VALUE));
		// Configuration conf = toUserVector.getConfiguration();
		// ConfigHelper.setInputInitialAddress(conf, "localhost");
		// ConfigHelper.setInputRpcPort(conf, "9160");
		// ConfigHelper.setInputSlicePredicate(conf, slice);
		// ConfigHelper.setInputColumnFamily(conf, "recommender", "users");
		// toUserVector.getConfiguration().setBoolean(BOOLEAN_DATA,
		// booleanData);
		// toUserVector.getConfiguration().setInt(
		// ToUserVectorsReducer.MIN_PREFERENCES_PER_USER,
		// minPrefsPerUser);
		// toUserVector.waitForCompletion(true);
		// numberOfUsers = (int) toUserVector.getCounters()
		// .findCounter(ToUserVectorsReducer.Counters.USERS)
		// .getValue();
		// }
		//
		//
		/********************************/

		AtomicInteger currentPhase = new AtomicInteger();

		int numberOfUsers = -1;

		if (shouldRunNextPhase(parsedArgs, currentPhase)) {
			ToolRunner.run(
					getConf(),
					new BaselinePreparePreferenceMatrixJob(),
					new String[] { "--keyspace", KEYSPACE, "--table", TABLE,
							"--output", prepPath.toString(),
							"--maxPrefsPerUser",
							String.valueOf(maxPrefsPerUserInItemSimilarity),
							"--minPrefsPerUser",
							String.valueOf(minPrefsPerUser), "--tempDir",
							getTempPath().toString() });

			numberOfUsers = HadoopUtil.readInt(new Path(prepPath,
					PreparePreferenceMatrixJob.NUM_USERS), getConf());
		}

		if (shouldRunNextPhase(parsedArgs, currentPhase)) {

			/* special behavior if phase 1 is skipped */
			if (numberOfUsers == -1) {
				numberOfUsers = (int) HadoopUtil.countRecords(new Path(
						prepPath,
						BaselinePreparePreferenceMatrixJob.USER_VECTORS),
						PathType.LIST, null, getConf());
			}

			/*
			 * Once DistributedRowMatrix uses the hadoop 0.20 API, we should
			 * refactor this call to something like new
			 * DistributedRowMatrix(...).rowSimilarity(...)
			 */
			// calculate the co-occurrence matrix
			ToolRunner.run(
					getConf(),
					new RowSimilarityJob(),
					new String[] {
							"--input",
							new Path(prepPath,
									PreparePreferenceMatrixJob.RATING_MATRIX)
									.toString(), "--output",
							similarityMatrixPath.toString(),
							"--numberOfColumns", String.valueOf(numberOfUsers),
							"--similarityClassname", similarityClassname,
							"--maxSimilaritiesPerRow",
							String.valueOf(maxSimilaritiesPerItem),
							"--excludeSelfSimilarity",
							String.valueOf(Boolean.TRUE), "--threshold",
							String.valueOf(threshold), "--tempDir",
							getTempPath().toString() });
		}

		/**************************/
		//
		// // start the multiplication of the co-occurrence matrix by the user
		// // vectors
		// if (shouldRunNextPhase(parsedArgs, currentPhase)) {
		// Job prePartialMultiply1 = prepareJob(similarityMatrixPath,
		// prePartialMultiplyPath1, SequenceFileInputFormat.class,
		// SimilarityMatrixRowWrapperMapper.class,
		// VarIntWritable.class, VectorOrPrefWritable.class,
		// Reducer.class, VarIntWritable.class,
		// VectorOrPrefWritable.class, SequenceFileOutputFormat.class);
		// boolean succeeded = prePartialMultiply1.waitForCompletion(true);
		// if (!succeeded)
		// return -1;
		// // continue the multiplication
		// Job prePartialMultiply2 = prepareJob(new Path(prepPath,
		// PreparePreferenceMatrixJob.USER_VECTORS),
		// prePartialMultiplyPath2, SequenceFileInputFormat.class,
		// UserVectorSplitterMapper.class, VarIntWritable.class,
		// VectorOrPrefWritable.class, Reducer.class,
		// VarIntWritable.class, VectorOrPrefWritable.class,
		// SequenceFileOutputFormat.class);
		// if (usersFile != null) {
		// prePartialMultiply2.getConfiguration().set(
		// UserVectorSplitterMapper.USERS_FILE, usersFile);
		// }
		// prePartialMultiply2.getConfiguration().setInt(
		// UserVectorSplitterMapper.MAX_PREFS_PER_USER_CONSIDERED,
		// maxPrefsPerUser);
		// succeeded = prePartialMultiply2.waitForCompletion(true);
		// if (!succeeded)
		// return -1;
		// // finish the job
		// Job partialMultiply = prepareJob(new Path(prePartialMultiplyPath1
		// + "," + prePartialMultiplyPath2), partialMultiplyPath,
		// SequenceFileInputFormat.class, Mapper.class,
		// VarIntWritable.class, VectorOrPrefWritable.class,
		// ToVectorAndPrefReducer.class, VarIntWritable.class,
		// VectorAndPrefsWritable.class,
		// SequenceFileOutputFormat.class);
		// setS3SafeCombinedInputPath(partialMultiply, getTempPath(),
		// prePartialMultiplyPath1, prePartialMultiplyPath2);
		// succeeded = partialMultiply.waitForCompletion(true);
		// if (!succeeded)
		// return -1;
		// }
		//
		// if (shouldRunNextPhase(parsedArgs, currentPhase)) {
		// // filter out any users we don't care about
		// /*
		// * convert the user/item pairs to filter if a filterfile has been
		// * specified
		// */
		// if (filterFile != null) {
		// Job itemFiltering = prepareJob(new Path(filterFile),
		// explicitFilterPath, TextInputFormat.class,
		// ItemFilterMapper.class, VarLongWritable.class,
		// VarLongWritable.class,
		// ItemFilterAsVectorAndPrefsReducer.class,
		// VarIntWritable.class, VectorAndPrefsWritable.class,
		// SequenceFileOutputFormat.class);
		// boolean succeeded = itemFiltering.waitForCompletion(true);
		// if (!succeeded)
		// return -1;
		// }
		//
		// String aggregateAndRecommendInput = partialMultiplyPath.toString();
		// if (filterFile != null) {
		// aggregateAndRecommendInput += "," + explicitFilterPath;
		// }
		// // extract out the recommendations
		// Job aggregateAndRecommend = prepareJob(new Path(
		// aggregateAndRecommendInput), outputPath,
		// SequenceFileInputFormat.class, PartialMultiplyMapper.class,
		// VarLongWritable.class,
		// PrefAndSimilarityColumnWritable.class,
		// AggregateAndRecommendReducer.class, VarLongWritable.class,
		// RecommendedItemsWritable.class, TextOutputFormat.class);
		// Configuration aggregateAndRecommendConf = aggregateAndRecommend
		// .getConfiguration();
		// if (itemsFile != null) {
		// aggregateAndRecommendConf.set(
		// AggregateAndRecommendReducer.ITEMS_FILE, itemsFile);
		// }
		//
		// if (filterFile != null) {
		// setS3SafeCombinedInputPath(aggregateAndRecommend,
		// getTempPath(), partialMultiplyPath, explicitFilterPath);
		// }
		// setIOSort(aggregateAndRecommend);
		// aggregateAndRecommendConf.set(
		// AggregateAndRecommendReducer.ITEMID_INDEX_PATH, new Path(
		// prepPath, PreparePreferenceMatrixJob.ITEMID_INDEX)
		// .toString());
		// aggregateAndRecommendConf.setInt(
		// AggregateAndRecommendReducer.NUM_RECOMMENDATIONS,
		// numRecommendations);
		// aggregateAndRecommendConf.setBoolean(BOOLEAN_DATA, booleanData);
		// boolean succeeded = aggregateAndRecommend.waitForCompletion(true);
		// if (!succeeded)
		// return -1;
		// }
		//
		// return 0;
		// }
		//
		// private static void setIOSort(JobContext job) {
		// Configuration conf = job.getConfiguration();
		// conf.setInt("io.sort.factor", 100);
		// String javaOpts = conf.get("mapred.map.child.java.opts"); // new arg
		// // name
		// if (javaOpts == null) {
		// javaOpts = conf.get("mapred.child.java.opts"); // old arg name
		// }
		// int assumedHeapSize = 512;
		// if (javaOpts != null) {
		// Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(
		// javaOpts);
		// if (m.find()) {
		// assumedHeapSize = Integer.parseInt(m.group(1));
		// String megabyteOrGigabyte = m.group(2);
		// if ("g".equalsIgnoreCase(megabyteOrGigabyte)) {
		// assumedHeapSize *= 1024;
		// }
		// }
		// }
		// // Cap this at 1024MB now; see
		// // https://issues.apache.org/jira/browse/MAPREDUCE-2308
		// conf.setInt("io.sort.mb", Math.min(assumedHeapSize / 2, 1024));
		// // For some reason the Merger doesn't report status for a long time;
		// // increase
		// // timeout when running these jobs
		// conf.setInt("mapred.task.timeout", 60 * 60 * 1000);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Hello world!");
		ToolRunner.run(new Configuration(), new BaselineRecommenderJob(), args);
		System.out.println("Bye world!");
	}

}
