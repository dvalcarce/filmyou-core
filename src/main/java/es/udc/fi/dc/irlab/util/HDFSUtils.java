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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * Utility class for HDFS.
 * 
 */
public class HDFSUtils {

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
     * Open a SequenceFile and read a float.
     * 
     * @param path
     * @param baseDirectory
     * @return
     * @throws IOException
     */
    public static double getDoubleFromSequenceFile(Path path,
	    String baseDirectory) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(baseDirectory + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    NullWritable key = NullWritable.get();
	    DoubleWritable val = new DoubleWritable();

	    if (reader.next(key, val)) {
		return val.get();
	    }

	    throw new IOException();
	}

    }

}
