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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderJob;

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

	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	try {
	    FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		    mergedFile, conf)) {

		NullWritable key = NullWritable.get();
		DoubleWritable val = new DoubleWritable();

		if (reader.next(key, val)) {
		    return val.get();
		}

	    }
	} finally {
	    removeData(conf, mergedFile.toString());
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
	if (conf.getBoolean(RMRecommenderJob.useCassandra, true)) {
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
	if (conf.getBoolean(RMRecommenderJob.useCassandra, true)) {
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

	for (Entry<String, String> entry : newConf) {
	    key = entry.getKey();
	    if (key.equals(inputPathName) || key.equals(outputPathName)) {
		continue;
	    }
	    newConf.set(entry.getKey(), entry.getValue());
	}

	return newConf;

    }
}
