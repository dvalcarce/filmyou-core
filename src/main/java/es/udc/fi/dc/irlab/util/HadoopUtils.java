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

package es.udc.fi.dc.irlab.util;

import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;

/**
 * Utility class for HDFS.
 * 
 */
public final class HadoopUtils {

	public static final String inputPathName = "mapred.input.dir";
	public static final String outputPathName = "mapred.output.dir";

	private HadoopUtils() {

	}

	/**
	 * Remove data from HDFS
	 * 
	 * @param conf
	 *            Configuration file
	 * @param path
	 *            path of the data to be removed
	 * @throws IOException
	 */
	public static void removeData(Configuration conf, String path)
			throws IOException {
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		fs.delete(new Path(path), true);
	}

	/**
	 * Open a SequenceFile and read a double.
	 * 
	 * @param path
	 * @param baseDirectory
	 * @return the double
	 * @throws IOException
	 */
	public static double getDoubleFromSequenceFile(Configuration conf,
			Path path, String baseDirectory) throws IOException {

		Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

		NullWritable key = NullWritable.get();
		DoubleWritable val = new DoubleWritable();

		for (Reader reader : readers) {
			if (reader.next(key, val)) {
				return val.get();
			}
		}

		throw new IOException();

	}

	/**
	 * Create a file which merges all the files in the directory provided.
	 * 
	 * @param conf
	 *            Configuration file
	 * @param pathToMerge
	 *            directory to be merged
	 * @param directory
	 *            destiny directory
	 * @param name
	 *            name of the merged file
	 * @return
	 * @throws IOException
	 */
	public static Path mergeFile(Configuration conf, Path pathToMerge,
			String directory, String name) throws IOException {

		FileSystem fs = FileSystem.get(conf);

		Path mergedFile = new Path(directory + "/" + name);
		if (!FileUtil.copyMerge(fs, pathToMerge, fs, mergedFile, false, conf,
				null)) {
			throw new IllegalArgumentException(pathToMerge + " is a folder!");
		}
		conf.set(name, mergedFile.toString());

		return mergedFile;

	}

	/**
	 * Create a folder and all non-existent parents.
	 * 
	 * @param conf
	 *            Configuration
	 * @param folder
	 *            folder to create
	 * @throws IOException
	 */
	public static void createFolder(Configuration conf, String folder)
			throws IOException {

		FileSystem fs = FileSystem.get(conf);

		fs.mkdirs(new Path(folder));

	}

	/**
	 * Get the inputPath for the job of the given conf
	 * 
	 * @param conf
	 *            Job Configuration
	 * @return the path
	 */
	public static Path getInputPath(Configuration conf) {
		if (conf.getBoolean(RMRecommenderDriver.useCassandra, true)) {
			return null;
		}
		return new Path(conf.get(HadoopUtils.inputPathName));
	}

	/**
	 * Get the outnputPath for the job of the given conf
	 * 
	 * @param conf
	 *            Job Configuration
	 * @return the path
	 */
	public static Path getOutputPath(Configuration conf) {
		if (conf.getBoolean(RMRecommenderDriver.useCassandra, true)) {
			return null;
		}
		return new Path(conf.get(HadoopUtils.outputPathName));
	}

	/**
	 * Remove input/output information from Configuration object
	 * 
	 * @param conf
	 *            Configuration object
	 * @return sanitized object
	 */
	public static Configuration sanitizeConf(Configuration conf) {
		String key;
		Configuration newConf = new Configuration();

		for (Entry<String, String> entry : conf) {
			key = entry.getKey();
			if (key.equals(inputPathName) || key.equals(outputPathName)) {
				continue;
			}
			newConf.set(key, entry.getValue());
		}

		return newConf;

	}

	/**
	 * Send the data contained in the given path via DistributedCache. If path
	 * is a directory, send the sequence files contained.
	 * 
	 * @param path
	 *            Path to send
	 * @param jobConf
	 *            Job Configuration object
	 * @return the number of added files
	 * @throws IOException
	 */
	public static int addDistributedFolder(Path path, Configuration jobConf)
			throws IOException {

		FileSystem fs = path.getFileSystem(jobConf);
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(final Path path) {
				final String name = path.getName();
				return !name.startsWith("_") && !name.startsWith(".");
			}
		};

		Path[] paths = FileUtil.stat2Paths(fs.listStatus(path, filter));

		for (Path p : paths) {
			DistributedCache.addCacheFile(p.toUri(), jobConf);
		}

		return paths.length;

	}

	/**
	 * Open the folder output generated by a SequenceFileOutputFormat in the
	 * local file system.
	 * 
	 * @param directory
	 *            SequenceFileOutputFormat output
	 * @param conf
	 *            Job Configuration
	 * @return the readers
	 * @throws IOException
	 */
	public static SequenceFile.Reader[] getLocalSequenceReaders(Path directory,
			Configuration conf) throws IOException {

		FileSystem fs = FileSystem.getLocal(conf);
		return _getSequenceReaders(directory, conf, fs);

	}

	/**
	 * Open the folder output generated by a SequenceFileOutputFormat in the
	 * distributed file system.
	 * 
	 * @param directory
	 *            SequenceFileOutputFormat output
	 * @param conf
	 *            Job Configuration
	 * @return the readers
	 * @throws IOException
	 */
	public static SequenceFile.Reader[] getSequenceReaders(Path directory,
			Configuration conf) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		return _getSequenceReaders(directory, conf, fs);

	}

	private static SequenceFile.Reader[] _getSequenceReaders(Path directory,
			Configuration conf, FileSystem fs) throws IOException {

		// Fix for bug MAPREDUCE-5448
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(final Path path) {
				final String name = path.getName();
				return !name.startsWith("_") && !name.startsWith(".");
			}
		};
		Path[] names = FileUtil.stat2Paths(fs.listStatus(directory, filter));

		SequenceFile.Reader[] parts = new SequenceFile.Reader[names.length];
		for (int i = 0; i < names.length; i++) {
			parts[i] = new SequenceFile.Reader(fs, names[i], conf);
		}

		return parts;

	}

}
