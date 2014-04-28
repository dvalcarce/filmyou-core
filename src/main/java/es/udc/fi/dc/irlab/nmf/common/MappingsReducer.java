package es.udc.fi.dc.irlab.nmf.common;

import es.udc.fi.dc.irlab.nmf.MatrixComputationJob;
import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

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
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		int iteration = conf.getInt(RMRecommenderDriver.iteration, -1);
		int subClustering = conf.getInt(RMRecommenderDriver.subClustering, -1);
		int numberOfIterations = conf.getInt(
				RMRecommenderDriver.numberOfIterations, -1);

		if (subClustering >= 0 && iteration == numberOfIterations) {
			loadMappings(conf,
					conf.getInt(MatrixComputationJob.numberOfFiles, -1));
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
	protected void loadMappings(Configuration conf, int length)
			throws IOException, InterruptedException {

		Path[] paths = DistributedCache.getLocalCacheFiles(conf);

		if (paths == null || paths.length != length) {
			throw new FileNotFoundException(length
					+ " files required not found");
		}

		userMap = new TIntIntHashMap(conf.getInt(
				RMRecommenderDriver.numberOfUsers, 0));

		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.getLocal(conf), paths[length - 1], conf)) {

			IntWritable key = new IntWritable();
			IntWritable val = new IntWritable();

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
	protected int getOldUserId(int user) {
		if (userMap != null) {
			return userMap.get(user);
		}
		return user;
	}

}
