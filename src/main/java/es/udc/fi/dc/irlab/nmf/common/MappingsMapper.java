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
import org.apache.hadoop.mapreduce.Mapper;

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
public class MappingsMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {

	private TIntIntMap userMap;
	private TIntIntMap itemMap;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();

		if (conf.getInt(RMRecommenderDriver.subClustering, -1) >= 0) {
			loadMappings(conf,
					conf.getInt(MatrixComputationJob.numberOfFiles, -1));
		}

	}

	/**
	 * Load user and item mappings located at the last distributed cache file.
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
			throw new FileNotFoundException("Found " + paths + ", " + length
					+ " files required");
		}

		userMap = new TIntIntHashMap(conf.getInt(
				RMRecommenderDriver.numberOfUsers, 0));
		itemMap = new TIntIntHashMap(conf.getInt(
				RMRecommenderDriver.numberOfItems, 0));

		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.getLocal(conf), paths[length - 2], conf)) {

			IntWritable key = new IntWritable();
			IntWritable val = new IntWritable();

			while (reader.next(key, val)) {
				userMap.put(key.get(), val.get());
			}

		}

		try (SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.getLocal(conf), paths[length - 1], conf)) {

			IntWritable key = new IntWritable();
			IntWritable val = new IntWritable();

			while (reader.next(key, val)) {
				itemMap.put(key.get(), val.get());
			}

		}

	}

	/**
	 * Indicates if there exist mappings.
	 * 
	 * @return true if exists, false otherwise
	 */
	protected boolean existsMapping() {
		return userMap != null;
	}

	/**
	 * Get the new user ID for the given old user ID. Be careful: this method
	 * assumes there exists a user mapping.
	 * 
	 * @param user
	 *            old user ID
	 * @return the new user ID
	 */
	protected int getNewUserId(int user) {
		if (userMap.containsKey(user)) {
			return userMap.get(user);
		}
		return -1;
	}

	/**
	 * Get the new item ID for the given old item ID. Be careful: this method
	 * assumes there exists a item mapping.
	 * 
	 * @param item
	 *            old user ID
	 * @return the new user ID
	 */
	protected int getNewItemId(int item) {
		if (itemMap.containsKey(item)) {
			return itemMap.get(item);
		}
		return -1;
	}

}
