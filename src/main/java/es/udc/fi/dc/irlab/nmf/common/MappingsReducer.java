package es.udc.fi.dc.irlab.nmf.common;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Load user mapping.
 *
 * @param <K1>
 *            input key
 * @param <V1>
 *            input value
 * @param <K2>
 *            output key
 * @param <V2>
 *            output value
 */
public class MappingsReducer<K1, V1, K2, V2> extends Reducer<K1, V1, K2, V2> {

    private TIntIntMap userMap;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

        final Configuration conf = context.getConfiguration();
        final int iteration = conf.getInt(RMRecommenderDriver.iteration, -1);
        final int subClustering = conf.getInt(RMRecommenderDriver.subClustering, -1);
        final int numberOfIterations = conf.getInt(RMRecommenderDriver.numberOfIterations, -1);

        if (subClustering >= 0 && iteration == numberOfIterations) {
            loadMappings(conf, conf.getInt(MatrixComputationJob.numberOfFiles, -1));
        }

    }

    /**
     * Load user mapping located at the last distributed cache file.
     *
     * @param conf
     *            Job Configuration
     * @param length
     *            number of distributed cache files
     * @throws IOException
     * @throws InterruptedException
     */
    protected void loadMappings(final Configuration conf, final int length)
            throws IOException, InterruptedException {

        final Path[] paths = DistributedCache.getLocalCacheFiles(conf);

        if (paths == null || paths.length != length) {
            throw new FileNotFoundException(length + " files required not found");
        }

        userMap = new TIntIntHashMap(conf.getInt(RMRecommenderDriver.numberOfUsers, 0));

        try (SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(conf),
                paths[length - 1], conf)) {

            final IntWritable key = new IntWritable();
            final IntWritable val = new IntWritable();

            while (reader.next(key, val)) {
                userMap.put(val.get(), key.get());
            }

        }

    }

    /**
     * Get the old user ID for the given new user ID. If there exists no
     * mapping, the same id is returned.
     *
     * @param user
     *            old user ID
     * @return the new user ID
     */
    protected int getOldUserId(final int user) {
        if (userMap != null) {
            return userMap.get(user);
        }
        return user;
    }

}
