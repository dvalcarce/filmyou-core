/**
 * Copyright 2013 Daniel Valcarce Silva
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
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;

import es.udc.fi.dc.irlab.nmf.util.AbstractJoinVectorMapper;

/**
 * Emit &lt;j, A_{i,j}w_i^T)> from HDFS ratings ({A_{i,j}}) and from H matrix
 * (w_i) obtained by DistributedCache.
 */
public class VectorByMovieCassandraMapper
	extends
	AbstractJoinVectorMapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>> {

    @Override
    protected void map(Map<String, ByteBuffer> keys,
	    Map<String, ByteBuffer> columns, Context context)
	    throws IOException, InterruptedException {

	float score = columns.get("score").getFloat();

	if (score > 0) {
	    context.write(new IntWritable(keys.get("user").getInt()),
		    new VectorWritable(getCache(keys.get("movie").getInt())
			    .times(score)));
	}

    }
}