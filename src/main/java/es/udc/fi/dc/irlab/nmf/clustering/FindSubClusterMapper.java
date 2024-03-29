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

package es.udc.fi.dc.irlab.nmf.clustering;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;

/**
 * Find the subcluster of each user.
 *
 * Emit &lt;j, k> from &lt;j, h_j> where
 *
 * k' = arg max (h_j) + k * numberOfSubClusters
 */
public class FindSubClusterMapper
        extends Mapper<IntWritable, VectorWritable, IntWritable, IntWritable> {

    private int numberOfSubClusters;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

        final Configuration conf = context.getConfiguration();

        final int numberOfUsers = conf.getInt(RMRecommenderDriver.numberOfUsers, -1);
        final int numberOfClusters = conf.getInt(RMRecommenderDriver.numberOfClusters, -1);
        numberOfSubClusters = (int) Math.ceil(numberOfUsers / (double) numberOfClusters);

    }

    @Override
    protected void map(final IntWritable user, final VectorWritable value, final Context context)
            throws IOException, InterruptedException {

        int cluster;
        final Vector vector = value.get();
        final int subCluster = vector.maxValueIndex();

        final String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();

        final Pattern pattern = Pattern.compile(File.separator + "cluster([0-9]+)");
        final Matcher matcher = pattern.matcher(filePathString);

        if (matcher.find()) {
            cluster = Integer.parseInt(matcher.group(1));
        } else {
            throw new FileNotFoundException(filePathString);
        }

        context.write(user, new IntWritable(cluster * numberOfSubClusters + subCluster));

    }

}
