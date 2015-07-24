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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

import es.udc.fi.dc.irlab.util.HadoopUtils;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Abstract mapper for reading clustering data as a SequenceFile in HDFS.
 *
 * @param <A>
 *            Mapper input key class
 * @param <B>
 *            Mapper input value class
 */
public abstract class AbstractByClusterMapper<A, B, C, D> extends Mapper<A, B, C, D> {

    private Path[] paths;
    private final TIntIntMap userClusterMap = new TIntIntHashMap();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

        final Configuration conf = context.getConfiguration();

        paths = DistributedCache.getLocalCacheFiles(conf);

        if (paths == null || paths.length < 1) {
            throw new FileNotFoundException();
        }

        final Reader[] readers = HadoopUtils.getLocalSequenceReaders(paths[0], conf);

        final IntWritable key = new IntWritable();
        final IntWritable val = new IntWritable();

        for (final Reader reader : readers) {
            while (reader.next(key, val)) {
                userClusterMap.put(key.get(), val.get());
            }
        }

    }

    /**
     * Get the cluster for the given user.
     *
     * @param user
     *            user id
     * @return cluster
     */
    protected int getCluster(final int user) {
        return userClusterMap.get(user);
    }

}
