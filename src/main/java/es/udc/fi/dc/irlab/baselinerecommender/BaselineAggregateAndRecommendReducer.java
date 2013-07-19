package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.MutableRecommendedItem;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.hadoop.TopItemsQueue;
import org.apache.mahout.cf.taste.hadoop.item.AggregateAndRecommendReducer;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaselineAggregateAndRecommendReducer
	extends
	Reducer<VarLongWritable, PrefAndSimilarityColumnWritable, Map<String, ByteBuffer>, ByteBuffer> {
    private static final Logger log = LoggerFactory
	    .getLogger(AggregateAndRecommendReducer.class);

    static final String ITEMID_INDEX_PATH = "itemIDIndexPath";
    static final String NUM_RECOMMENDATIONS = "numRecommendations";
    static final int DEFAULT_NUM_RECOMMENDATIONS = 10;
    static final String ITEMS_FILE = "itemsFile";

    private boolean booleanData;
    private int recommendationsPerUser;
    private FastIDSet itemsToRecommendFor;
    private OpenIntLongHashMap indexItemIDMap;

    private static final float BOOLEAN_PREF_VALUE = 1.0f;

    @Override
    protected void setup(Context context) throws IOException {
	Configuration conf = context.getConfiguration();
	recommendationsPerUser = conf.getInt(NUM_RECOMMENDATIONS,
		DEFAULT_NUM_RECOMMENDATIONS);
	booleanData = conf.getBoolean(RecommenderJob.BOOLEAN_DATA, false);
	indexItemIDMap = TasteHadoopUtils.readIDIndexMap(
		conf.get(ITEMID_INDEX_PATH), conf);

	String itemFilePathString = conf.get(ITEMS_FILE);
	if (itemFilePathString != null) {
	    itemsToRecommendFor = new FastIDSet();
	    for (String line : new FileLineIterable(HadoopUtil.openStream(
		    new Path(itemFilePathString), conf))) {
		try {
		    itemsToRecommendFor.add(Long.parseLong(line));
		} catch (NumberFormatException nfe) {
		    log.warn("itemsFile line ignored: {}", line);
		}
	    }
	}
    }

    @Override
    protected void reduce(VarLongWritable userID,
	    Iterable<PrefAndSimilarityColumnWritable> values, Context context)
	    throws IOException, InterruptedException {
	if (booleanData) {
	    reduceBooleanData(userID, values, context);
	} else {
	    reduceNonBooleanData(userID, values, context);
	}
    }

    private void reduceBooleanData(VarLongWritable userID,
	    Iterable<PrefAndSimilarityColumnWritable> values, Context context)
	    throws IOException, InterruptedException {
	/*
	 * having boolean data, each estimated preference can only be 1, however
	 * we can't use this to rank the recommended items, so we use the sum of
	 * similarities for that.
	 */
	Iterator<PrefAndSimilarityColumnWritable> columns = values.iterator();
	Vector predictions = columns.next().getSimilarityColumn();
	while (columns.hasNext()) {
	    predictions.assign(columns.next().getSimilarityColumn(),
		    Functions.PLUS);
	}
	writeRecommendedItems(userID, predictions, context);
    }

    private void reduceNonBooleanData(VarLongWritable userID,
	    Iterable<PrefAndSimilarityColumnWritable> values, Context context)
	    throws IOException, InterruptedException {
	/* each entry here is the sum in the numerator of the prediction formula */
	Vector numerators = null;
	/*
	 * each entry here is the sum in the denominator of the prediction
	 * formula
	 */
	Vector denominators = null;
	/*
	 * each entry here is the number of similar items used in the prediction
	 * formula
	 */
	Vector numberOfSimilarItemsUsed = new RandomAccessSparseVector(
		Integer.MAX_VALUE, 100);

	for (PrefAndSimilarityColumnWritable prefAndSimilarityColumn : values) {
	    Vector simColumn = prefAndSimilarityColumn.getSimilarityColumn();
	    float prefValue = prefAndSimilarityColumn.getPrefValue();
	    /* count the number of items used for each prediction */
	    for (Element e : simColumn.nonZeroes()) {
		int itemIDIndex = e.index();
		numberOfSimilarItemsUsed.setQuick(itemIDIndex,
			numberOfSimilarItemsUsed.getQuick(itemIDIndex) + 1);
	    }

	    if (denominators == null) {
		denominators = simColumn.clone();
	    } else {
		denominators.assign(simColumn, Functions.PLUS_ABS);
	    }

	    if (numerators == null) {
		numerators = simColumn.clone();
		if (prefValue != BOOLEAN_PREF_VALUE) {
		    numerators.assign(Functions.MULT, prefValue);
		}
	    } else {
		if (prefValue != BOOLEAN_PREF_VALUE) {
		    simColumn.assign(Functions.MULT, prefValue);
		}
		numerators.assign(simColumn, Functions.PLUS);
	    }

	}

	if (numerators == null) {
	    return;
	}

	Vector recommendationVector = new RandomAccessSparseVector(
		Integer.MAX_VALUE, 100);
	for (Element element : numerators.nonZeroes()) {
	    int itemIDIndex = element.index();
	    /* preference estimations must be based on at least 2 datapoints */
	    if (numberOfSimilarItemsUsed.getQuick(itemIDIndex) > 1) {
		/* compute normalized prediction */
		double prediction = element.get()
			/ denominators.getQuick(itemIDIndex);
		recommendationVector.setQuick(itemIDIndex, prediction);
	    }
	}
	writeRecommendedItems(userID, recommendationVector, context);
    }

    /**
     * Find the top entries in recommendationVector, map them to the real
     * itemIDs and write back the result.
     * 
     * @param userID
     *            target user
     * @param recommendationVector
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeRecommendedItems(VarLongWritable userID,
	    Vector recommendationVector, Context context) throws IOException,
	    InterruptedException {

	TopItemsQueue topKItems = new TopItemsQueue(recommendationsPerUser);

	for (Element element : recommendationVector.nonZeroes()) {
	    int index = element.index();
	    long itemID;
	    if (indexItemIDMap != null && !indexItemIDMap.isEmpty()) {
		itemID = indexItemIDMap.get(index);
	    } else { // we don't have any mappings, so just use the original
		itemID = index;
	    }
	    if (itemsToRecommendFor == null
		    || itemsToRecommendFor.contains(itemID)) {
		float value = (float) element.get();
		if (!Float.isNaN(value)) {
		    MutableRecommendedItem topItem = topKItems.top();
		    if (value > topItem.getValue()) {
			topItem.set(itemID, value);
			topKItems.updateTop();
		    }
		}
	    }
	}
	List<RecommendedItem> topItems = topKItems.getTopItems();
	if (!topItems.isEmpty()) {
	    for (RecommendedItem item : topItems) {
		Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
		keys.put("user", ByteBufferUtil.bytes((int) userID.get()));
		keys.put("movie", ByteBufferUtil.bytes((int) item.getItemID()));
		context.write(keys,
			ByteBufferUtil.bytes((int) Math.round(item.getValue())));
	    }
	}
    }
}
