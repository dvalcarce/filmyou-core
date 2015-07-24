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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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

/**
 * Final stage of the baseline recommendation algorithm.
 *
 */
public class BaselineAggregateAndRecommendReducer extends
        Reducer<VarLongWritable, PrefAndSimilarityColumnWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
    private static final Logger log = LoggerFactory.getLogger(AggregateAndRecommendReducer.class);

    final static String ITEMID_INDEX_PATH = "itemIDIndexPath";
    final static String NUM_RECOMMENDATIONS = "numRecommendations";
    final static int DEFAULT_NUM_RECOMMENDATIONS = 10;
    final static String ITEMS_FILE = "itemsFile";

    private boolean booleanData;
    private int recommendationsPerUser;
    private FastIDSet itemsToRecommendFor;
    private OpenIntLongHashMap indexItemIDMap;

    private static final float BOOLEAN_PREF_VALUE = 1.0f;

    @Override
    protected void reduce(final VarLongWritable userID,
            final Iterable<PrefAndSimilarityColumnWritable> values, final Context context)
                    throws IOException, InterruptedException {
        if (booleanData) {
            reduceBooleanData(userID, values, context);
        } else {
            reduceNonBooleanData(userID, values, context);
        }
    }

    private void reduceBooleanData(final VarLongWritable userID,
            final Iterable<PrefAndSimilarityColumnWritable> values, final Context context)
                    throws IOException, InterruptedException {
        /*
         * having boolean data, each estimated preference can only be 1, however
         * we can't use this to rank the recommended items, so we use the sum of
         * similarities for that.
         */
        final Iterator<PrefAndSimilarityColumnWritable> columns = values.iterator();
        final Vector predictions = columns.next().getSimilarityColumn();
        while (columns.hasNext()) {
            predictions.assign(columns.next().getSimilarityColumn(), Functions.PLUS);
        }
        writeRecommendedItems(userID, predictions, context);
    }

    private void reduceNonBooleanData(final VarLongWritable userID,
            final Iterable<PrefAndSimilarityColumnWritable> values, final Context context)
                    throws IOException, InterruptedException {
        /*
         * each entry here is the sum in the numerator of the prediction formula
         */
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
        final Vector numberOfSimilarItemsUsed = new RandomAccessSparseVector(Integer.MAX_VALUE,
                100);

        for (final PrefAndSimilarityColumnWritable prefAndSimilarityColumn : values) {
            final Vector simColumn = prefAndSimilarityColumn.getSimilarityColumn();
            final float prefValue = prefAndSimilarityColumn.getPrefValue();
            /* count the number of items used for each prediction */
            for (final Element e : simColumn.nonZeroes()) {
                final int itemIDIndex = e.index();
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

        final Vector recommendationVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        for (final Element element : numerators.nonZeroes()) {
            final int itemIDIndex = element.index();
            /* preference estimations must be based on at least 2 datapoints */
            if (numberOfSimilarItemsUsed.getQuick(itemIDIndex) > 1) {
                /* compute normalized prediction */
                final double prediction = element.get() / denominators.getQuick(itemIDIndex);
                recommendationVector.setQuick(itemIDIndex, prediction);
            }
        }
        writeRecommendedItems(userID, recommendationVector, context);
    }

    @Override
    protected void setup(final Context context) throws IOException {
        final Configuration conf = context.getConfiguration();
        recommendationsPerUser = conf.getInt(NUM_RECOMMENDATIONS, DEFAULT_NUM_RECOMMENDATIONS);
        booleanData = conf.getBoolean(RecommenderJob.BOOLEAN_DATA, false);
        indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(ITEMID_INDEX_PATH), conf);

        final String itemFilePathString = conf.get(ITEMS_FILE);
        if (itemFilePathString != null) {
            itemsToRecommendFor = new FastIDSet();
            for (final String line : new FileLineIterable(
                    HadoopUtil.openStream(new Path(itemFilePathString), conf))) {
                try {
                    itemsToRecommendFor.add(Long.parseLong(line));
                } catch (final NumberFormatException nfe) {
                    log.warn("itemsFile line ignored: {}", line);
                }
            }
        }
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
    private void writeRecommendedItems(final VarLongWritable userID,
            final Vector recommendationVector, final Context context)
                    throws IOException, InterruptedException {

        final TopItemsQueue topKItems = new TopItemsQueue(recommendationsPerUser);

        for (final Element element : recommendationVector.nonZeroes()) {
            final int index = element.index();
            long itemID;
            if (indexItemIDMap != null && !indexItemIDMap.isEmpty()) {
                itemID = indexItemIDMap.get(index);
            } else { // we don't have any mappings, so just use the original
                itemID = index;
            }
            if (itemsToRecommendFor == null || itemsToRecommendFor.contains(itemID)) {
                final float value = (float) element.get();
                if (!Float.isNaN(value)) {
                    final MutableRecommendedItem topItem = topKItems.top();
                    if (value > topItem.getValue()) {
                        topItem.set(itemID, value);
                        topKItems.updateTop();
                    }
                }
            }
        }

        final List<RecommendedItem> topItems = topKItems.getTopItems();
        if (!topItems.isEmpty()) {
            for (final RecommendedItem item : topItems) {
                final Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
                keys.put("user", ByteBufferUtil.bytes((int) userID.get()));
                keys.put("item", ByteBufferUtil.bytes((int) item.getItemID()));
                keys.put("score", ByteBufferUtil.bytes(item.getValue()));

                final List<ByteBuffer> value = new LinkedList<ByteBuffer>();
                value.add(ByteBufferUtil.bytes(0));

                context.write(keys, value);
            }
        }
    }

}
