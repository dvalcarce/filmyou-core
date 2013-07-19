package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable;
import org.apache.mahout.cf.taste.hadoop.item.ItemFilterAsVectorAndPrefsReducer;
import org.apache.mahout.cf.taste.hadoop.item.ItemFilterMapper;
import org.apache.mahout.cf.taste.hadoop.item.PartialMultiplyMapper;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.hadoop.item.SimilarityMatrixRowWrapperMapper;
import org.apache.mahout.cf.taste.hadoop.item.ToVectorAndPrefReducer;
import org.apache.mahout.cf.taste.hadoop.item.UserVectorSplitterMapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.cf.taste.hadoop.similarity.item.ItemSimilarityJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CooccurrenceCountSimilarity;

/**
 * Basic Collaborative Filtering algorithm implementation. Based on Mahout's
 * item recommendation algorithm (org.apache.mahout.cv.taste.hadoop.item).
 * 
 */
public class BaselineRecommenderJob extends AbstractJob {

    public static final String BOOLEAN_DATA = "booleanData";
    private static final int DEFAULT_NUM_RECOMMENDATIONS = 100;
    private static final int DEFAULT_MAX_SIMILARITIES_PER_ITEM = 100;
    private static final int DEFAULT_MAX_PREFS_PER_USER = 1000;
    private static final int DEFAULT_MIN_PREFS_PER_USER = 1;
    private static final int DEFAULT_MAX_PREFS_PER_USER_CONSIDERED = 50;
    private static final String ITEMS_FILE = "items.txt";
    private static final String ITEMID_INDEX_PATH = "itemIDIndexPath";
    private static final String NUM_RECOMMENDATIONS = "numRecommendations";
    private static final String USERS_FILE = "usersFile";
    private static final String MAX_PREFS_PER_USER_CONSIDERED = "maxPrefsPerUserConsidered";

    private int numRecommendations;
    private String usersFile;
    private String itemsFile;
    private String filterFile;
    private boolean booleanData;

    /*
     * Cassandra params
     */
    private static final String KEYSPACE = "demo";
    private static final String IN_TABLE = "ratings";
    private static final String OUT_TABLE = "recommendations";

    /**
     * Load default command line arguments.
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
	addOption("booleanData", "b", "Treat input as without pref values",
		Boolean.FALSE.toString());
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
	addOption("outputPathForSimilarityMatrix", "opfsm",
		"write the item similarity matrix to this path (optional)",
		false);
    }

    /**
     * Recommendation algorithm.
     */
    public int run(String[] args) throws Exception {
	loadDefaultSetup();

	Map<String, List<String>> parsedArgs = parseArguments(args, true, true);
	if (parsedArgs == null) {
	    return -1;
	}

	// outputPath = getOutputPath();
	numRecommendations = Integer.parseInt(getOption("numRecommendations"));
	usersFile = getOption("usersFile");
	itemsFile = getOption("itemsFile");
	filterFile = getOption("filterFile");
	booleanData = Boolean.valueOf(getOption("booleanData"));
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
	Path explicitFilterPath = getTempPath("explicitFilterPath");
	Path partialMultiplyPath = getTempPath("partialMultiply");

	AtomicInteger currentPhase = new AtomicInteger();

	int numberOfUsers = -1;

	if (shouldRunNextPhase(parsedArgs, currentPhase)) {
	    ToolRunner.run(
		    getConf(),
		    new BaselinePreparePreferenceMatrixJob(),
		    new String[] { "--output", prepPath.toString(),
			    "--maxPrefsPerUser",
			    String.valueOf(maxPrefsPerUserInItemSimilarity),
			    "--minPrefsPerUser",
			    String.valueOf(minPrefsPerUser), "--booleanData",
			    String.valueOf(booleanData), "--tempDir",
			    getTempPath().toString(), "--keyspace", KEYSPACE,
			    "--table", IN_TABLE });

	    numberOfUsers = HadoopUtil.readInt(new Path(prepPath,
		    BaselinePreparePreferenceMatrixJob.NUM_USERS), getConf());
	}

	if (shouldRunNextPhase(parsedArgs, currentPhase)) {

	    /* special behavior if phase 1 is skipped */
	    if (numberOfUsers == -1) {
		numberOfUsers = (int) HadoopUtil.countRecords(new Path(
			prepPath, PreparePreferenceMatrixJob.USER_VECTORS),
			PathType.LIST, null, getConf());
	    }

	    /*
	     * Once DistributedRowMatrix uses the hadoop 0.20 API, we should
	     * refactor this call to something like new
	     * DistributedRowMatrix(...).rowSimilarity(...)
	     */
	    // calculate the co-occurrence matrix
	    ToolRunner
		    .run(getConf(),
			    new RowSimilarityJob(),
			    new String[] {
				    "--input",
				    new Path(
					    prepPath,
					    BaselinePreparePreferenceMatrixJob.RATING_MATRIX)
					    .toString(), "--output",
				    similarityMatrixPath.toString(),
				    "--numberOfColumns",
				    String.valueOf(numberOfUsers),
				    "--similarityClassname",
				    similarityClassname,
				    "--maxSimilaritiesPerRow",
				    String.valueOf(maxSimilaritiesPerItem),
				    "--excludeSelfSimilarity",
				    String.valueOf(Boolean.TRUE),
				    "--threshold", String.valueOf(threshold),
				    "--tempDir", getTempPath().toString(), });

	    /*
	     * OutputSimilarityMatrix Job. Write out the similarity matrix if
	     * the user specified that.
	     */
	    if (hasOption("outputPathForSimilarityMatrix")) {
		Path outputPathForSimilarityMatrix = new Path(
			getOption("outputPathForSimilarityMatrix"));

		Job outputSimilarityMatrix = prepareJob(similarityMatrixPath,
			outputPathForSimilarityMatrix,
			SequenceFileInputFormat.class,
			ItemSimilarityJob.MostSimilarItemPairsMapper.class,
			EntityEntityWritable.class, DoubleWritable.class,
			ItemSimilarityJob.MostSimilarItemPairsReducer.class,
			EntityEntityWritable.class, DoubleWritable.class,
			TextOutputFormat.class);

		Configuration mostSimilarItemsConf = outputSimilarityMatrix
			.getConfiguration();
		mostSimilarItemsConf.set(
			ItemSimilarityJob.ITEM_ID_INDEX_PATH_STR, new Path(
				prepPath,
				PreparePreferenceMatrixJob.ITEMID_INDEX)
				.toString());
		mostSimilarItemsConf.setInt(
			ItemSimilarityJob.MAX_SIMILARITIES_PER_ITEM,
			maxSimilaritiesPerItem);
		outputSimilarityMatrix.waitForCompletion(true);
	    }
	}

	/*
	 * PartialMultiply Job. Start the multiplication of the co-occurrence
	 * matrix by the user vectors.
	 */
	if (shouldRunNextPhase(parsedArgs, currentPhase)) {
	    Job partialMultiply = new Job(getConf(), "partialMultiply");
	    Configuration partialMultiplyConf = partialMultiply
		    .getConfiguration();

	    MultipleInputs.addInputPath(partialMultiply, similarityMatrixPath,
		    SequenceFileInputFormat.class,
		    SimilarityMatrixRowWrapperMapper.class);
	    MultipleInputs.addInputPath(partialMultiply, new Path(prepPath,
		    BaselinePreparePreferenceMatrixJob.USER_VECTORS),
		    SequenceFileInputFormat.class,
		    UserVectorSplitterMapper.class);
	    partialMultiply.setJarByClass(ToVectorAndPrefReducer.class);
	    partialMultiply.setMapOutputKeyClass(VarIntWritable.class);
	    partialMultiply.setMapOutputValueClass(VectorOrPrefWritable.class);
	    partialMultiply.setReducerClass(ToVectorAndPrefReducer.class);
	    partialMultiply
		    .setOutputFormatClass(SequenceFileOutputFormat.class);
	    partialMultiply.setOutputKeyClass(VarIntWritable.class);
	    partialMultiply.setOutputValueClass(VectorAndPrefsWritable.class);
	    partialMultiplyConf.setBoolean("mapred.compress.map.output", true);
	    partialMultiplyConf.set("mapred.output.dir",
		    partialMultiplyPath.toString());

	    if (usersFile != null) {
		partialMultiplyConf.set(USERS_FILE, usersFile);
	    }

	    partialMultiplyConf.setInt(MAX_PREFS_PER_USER_CONSIDERED,
		    maxPrefsPerUser);

	    boolean succeeded = partialMultiply.waitForCompletion(true);
	    if (!succeeded) {
		return -1;
	    }
	}

	if (shouldRunNextPhase(parsedArgs, currentPhase)) {
	    /*
	     * Filter out any users we don't care about.
	     * 
	     * Convert the user/item pairs to filter if a filterfile has been
	     * specified.
	     */
	    if (filterFile != null) {
		Job itemFiltering = prepareJob(new Path(filterFile),
			explicitFilterPath, TextInputFormat.class,
			ItemFilterMapper.class, VarLongWritable.class,
			VarLongWritable.class,
			ItemFilterAsVectorAndPrefsReducer.class,
			VarIntWritable.class, VectorAndPrefsWritable.class,
			SequenceFileOutputFormat.class);
		boolean succeeded = itemFiltering.waitForCompletion(true);
		if (!succeeded) {
		    return -1;
		}
	    }

	    /*
	     * AggregateAndRecommend Job. Extract out the recommendations.
	     */
	    if (selectTopRecommendations(partialMultiplyPath,
		    explicitFilterPath, prepPath) < 0) {
		return -1;
	    }

	}

	return 0;
    }

    /**
     * Write top recommendations.
     * 
     * @param aggregateAndRecommendInput
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected int selectTopRecommendations(Path partialMultiplyPath,
	    Path explicitFilterPath, Path prepPath) throws IOException,
	    ClassNotFoundException, InterruptedException {

	String aggregateAndRecommendInput = partialMultiplyPath.toString();
	if (filterFile != null) {
	    aggregateAndRecommendInput += "," + explicitFilterPath;
	}

	Job aggregateAndRecommend = new Job(new Configuration(getConf()),
		"aggregateAndRecommend");

	aggregateAndRecommend.setMapperClass(PartialMultiplyMapper.class);
	aggregateAndRecommend
		.setReducerClass(BaselineAggregateAndRecommendReducer.class);
	aggregateAndRecommend.setJarByClass(BaselineRecommenderJob.class);

	aggregateAndRecommend.setMapOutputKeyClass(VarLongWritable.class);
	aggregateAndRecommend
		.setMapOutputValueClass(PrefAndSimilarityColumnWritable.class);
	aggregateAndRecommend.setOutputKeyClass(Map.class);
	aggregateAndRecommend.setOutputValueClass(ByteBuffer.class);

	aggregateAndRecommend
		.setInputFormatClass(SequenceFileInputFormat.class);
	aggregateAndRecommend.setOutputFormatClass(CqlOutputFormat.class);

	Configuration conf = aggregateAndRecommend.getConfiguration();
	conf.set("mapred.input.dir", aggregateAndRecommendInput);

	// Cassandra settings
	String port = "9160";
	String host = "127.0.0.1";
	ConfigHelper.setOutputRpcPort(conf, port);
	ConfigHelper.setOutputInitialAddress(conf, host);
	ConfigHelper.setOutputPartitioner(conf,
		"org.apache.cassandra.dht.Murmur3Partitioner");
	ConfigHelper.setOutputColumnFamily(conf, KEYSPACE, OUT_TABLE);

	String query = "INSERT INTO " + KEYSPACE + "." + OUT_TABLE
		+ " (user, movie, score) VALUES (?, ?, ?);";
	CqlConfigHelper.setOutputCql(conf, query);

	if (itemsFile != null) {
	    conf.set(ITEMS_FILE, itemsFile);
	}

	setIOSort(aggregateAndRecommend);
	conf.set(ITEMID_INDEX_PATH, new Path(prepPath,
		PreparePreferenceMatrixJob.ITEMID_INDEX).toString());
	conf.setInt(NUM_RECOMMENDATIONS, numRecommendations);
	conf.setBoolean(BOOLEAN_DATA, booleanData);
	boolean succeeded = aggregateAndRecommend.waitForCompletion(true);
	if (!succeeded) {
	    return -1;
	}

	return 0;
    }

    /**
     * Set heap size to 1024 MB.
     * 
     * @param job
     *            JobContext
     */
    private static void setIOSort(JobContext job) {
	Configuration conf = job.getConfiguration();
	conf.setInt("io.sort.factor", 100);
	String javaOpts = conf.get("mapred.map.child.java.opts"); // new arg
	// name
	if (javaOpts == null) {
	    javaOpts = conf.get("mapred.child.java.opts"); // old arg name
	}
	int assumedHeapSize = 512;
	if (javaOpts != null) {
	    Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(
		    javaOpts);
	    if (m.find()) {
		assumedHeapSize = Integer.parseInt(m.group(1));
		String megabyteOrGigabyte = m.group(2);
		if ("g".equalsIgnoreCase(megabyteOrGigabyte)) {
		    assumedHeapSize *= 1024;
		}
	    }
	}

	// Cap this at 1024MB now; see
	// https://issues.apache.org/jira/browse/MAPREDUCE-2308
	conf.setInt("io.sort.mb", Math.min(assumedHeapSize / 2, 1024));

	// For some reason the Merger doesn't report status for a long time;
	// increase
	// timeout when running these jobs
	conf.setInt("mapred.task.timeout", 60 * 60 * 1000);
    }

    /**
     * Main routine. Launch BaselineRecommenderJob job.
     * 
     * @param args
     *            command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new BaselineRecommenderJob(), args);
    }

}
