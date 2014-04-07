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

package es.udc.fi.dc.irlab.nmf.hcomputation;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.util.AbstractJoinVectorMapper;

/**
 * Emit &lt;j, A_{i,j}w_i^T)> from HDFS ratings (<(i, j), A_{i,j}>) and from H
 * matrix (w_i) obtained by DistributedCache.
 */
public class ScoreByMovieHDFSMapper extends
	AbstractJoinVectorMapper<IntPairWritable, FloatWritable> {

    @Override
    protected void map(IntPairWritable key, FloatWritable column,
	    Context context) throws IOException, InterruptedException {

	float score = column.get();

	if (score > 0) {
	    context.write(new IntWritable(key.getSecond()), new VectorWritable(
		    getCache(key.getFirst()).times(score)));
	}

    }

}
