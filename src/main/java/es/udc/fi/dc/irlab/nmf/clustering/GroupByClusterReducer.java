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

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.fi.dc.irlab.util.IntSetWritable;

/**
 * Group all users by cluster.
 * 
 * Emit <k, Set<j>> from <k, {j}>.
 * 
 */
public class GroupByClusterReducer extends
	Reducer<IntWritable, IntWritable, IntWritable, IntSetWritable> {

    @Override
    protected void reduce(IntWritable cluster, Iterable<IntWritable> users,
	    Context context) throws IOException, InterruptedException {

	IntSetWritable set = new IntSetWritable(users);

	context.write(cluster, set);

    }
}