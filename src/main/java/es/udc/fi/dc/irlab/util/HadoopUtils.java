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
    public static void removeData(final Configuration conf, final String path) throws IOException {
        final FileSystem fs = FileSystem.get(URI.create(path), conf);
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
    public static double getDoubleFromSequenceFile(final Configuration conf, final Path path,
            final String baseDirectory) throws IOException {

        final Reader[] readers = HadoopUtils.getSequenceReaders(path, conf);

        final NullWritable key = NullWritable.get();
        final DoubleWritable val = new DoubleWritable();

        for (final Reader reader : readers) {
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
    public static Path mergeFile(final Configuration conf, final Path pathToMerge,
            final String directory, final String name) throws IOException {

        final FileSystem fs = FileSystem.get(conf);

        final Path mergedFile = new Path(directory + "/" + name);
        if (!FileUtil.copyMerge(fs, pathToMerge, fs, mergedFile, false, conf, null)) {
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
    public static void createFolder(final Configuration conf, final String folder)
            throws IOException {

        final FileSystem fs = FileSystem.get(conf);

        fs.mkdirs(new Path(folder));

    }

    /**
     * Get the inputPath for the job of the given conf
     *
     * @param conf
     *            Job Configuration
     * @return the path
     */
    public static Path getInputPath(final Configuration conf) {
        if (conf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
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
    public static Path getOutputPath(final Configuration conf) {
        if (conf.getBoolean(RMRecommenderDriver.useCassandraInput, true)) {
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
    public static Configuration sanitizeConf(final Configuration conf) {
        String key;
        final Configuration newConf = new Configuration();

        for (final Entry<String, String> entry : conf) {
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
    public static int addDistributedFolder(final Path path, final Configuration jobConf)
            throws IOException {

        final FileSystem fs = path.getFileSystem(jobConf);
        final PathFilter filter = new PathFilter() {
            @Override
            public boolean accept(final Path path) {
                final String name = path.getName();
                return !name.startsWith("_") && !name.startsWith(".");
            }
        };

        final Path[] paths = FileUtil.stat2Paths(fs.listStatus(path, filter));

        for (final Path p : paths) {
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
    public static SequenceFile.Reader[] getLocalSequenceReaders(final Path directory,
            final Configuration conf) throws IOException {

        final FileSystem fs = FileSystem.getLocal(conf);
        return getSequenceReaders(directory, conf, fs);

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
    public static SequenceFile.Reader[] getSequenceReaders(final Path directory,
            final Configuration conf) throws IOException {

        final FileSystem fs = FileSystem.get(conf);
        return getSequenceReaders(directory, conf, fs);

    }

    /**
     * Auxiliary method for getting SequenceReaders.
     *
     * @param directory
     *            SequenceFileOutputFormat output
     * @param conf
     *            Job Configuration
     * @param fs
     *            FileSystem
     * @return the readers
     * @throws IOException
     */
    private static SequenceFile.Reader[] getSequenceReaders(final Path directory,
            final Configuration conf, final FileSystem fs) throws IOException {

        // Fix for bug MAPREDUCE-5448
        final PathFilter filter = new PathFilter() {
            @Override
            public boolean accept(final Path path) {
                final String name = path.getName();
                return !name.startsWith("_") && !name.startsWith(".");
            }
        };
        final Path[] names = FileUtil.stat2Paths(fs.listStatus(directory, filter));

        final SequenceFile.Reader[] parts = new SequenceFile.Reader[names.length];
        for (int i = 0; i < names.length; i++) {
            parts[i] = new SequenceFile.Reader(fs, names[i], conf);
        }

        return parts;

    }

}
