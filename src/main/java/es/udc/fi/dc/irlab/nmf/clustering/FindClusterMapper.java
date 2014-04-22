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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Find the cluster of each user.
 * 
 * Emit &lt;j, k> from &lt;j, h_j> where k = arg max (h_j).
 */
public class FindClusterMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, IntWritable> {

	@Override
	protected void map(IntWritable user, VectorWritable value, Context context)
			throws IOException, InterruptedException {

		Vector vector = value.get();
		int cluster = vector.maxValueIndex();

		context.write(user, new IntWritable(cluster));

	}

}
