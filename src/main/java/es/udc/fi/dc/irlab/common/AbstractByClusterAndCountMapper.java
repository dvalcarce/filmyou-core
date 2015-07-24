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

package es.udc.fi.dc.irlab.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Abstract mapper for reading clustering data as a SequenceFile in HDFS.
 *
 * @param <A>
 *            Mapper input key class
 * @param <B>
 *            Mapper input value class
 */
public abstract class AbstractByClusterAndCountMapper<A, B, C, D>
        extends AbstractByClusterMapper<A, B, C, D> {

    private int[] clusterSizes;
    private Configuration conf;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

        conf = context.getConfiguration();

        final Path[] paths = DistributedCache.getLocalCacheFiles(conf);

        if (paths == null || paths.length < 2) {
            throw new FileNotFoundException();
        }

        super.setup(context);

        final int nubmerOfClusters = conf.getInt(RMRecommenderDriver.numberOfClusters, -1);

        clusterSizes = new int[nubmerOfClusters];

        final SequenceFile.Reader[] readers = HadoopUtils.getLocalSequenceReaders(paths[1], conf);

        final IntWritable key = new IntWritable();
        final IntWritable val = new IntWritable();

        // Read cluster count
        for (final SequenceFile.Reader reader : readers) {
            while (reader.next(key, val)) {
                clusterSizes[key.get()] = val.get();
            }
        }

    }

    /**
     * Get the cluster split for the given user.
     *
     * @param user
     *            user id
     * @return cluster split
     */
    protected String[] getSplits(final int user) {
        final int clusterID = getCluster(user);
        final int clusterSize = clusterSizes[clusterID];
        final int clusterSplit = conf.getInt(RMRecommenderDriver.clusterSplit, -1);
        final int splitSize = conf.getInt(RMRecommenderDriver.splitSize, -1);

        if (clusterSize < clusterSplit) {
            return new String[] { String.valueOf(clusterID) };
        }
        final List<String> splits = new ArrayList<String>();
        final int numberOfSplits = (int) Math.ceil(clusterSize / (double) splitSize);
        for (int split = 0; split < numberOfSplits; split++) {
            splits.add(String.format(Locale.ENGLISH, "%d-%d-%d", clusterID, split, numberOfSplits));
        }

        return splits.toArray(new String[0]);
    }

}
