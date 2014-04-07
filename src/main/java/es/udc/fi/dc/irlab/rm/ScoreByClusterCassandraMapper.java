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

package es.udc.fi.dc.irlab.rm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.mahout.common.IntPairWritable;

/**
 * Emit &lt;(k, 2), (i, j, A_{i,j})> from Cassandra ratings ({A_{i,j}}) where j
 * is a user from the cluster k.
 */
public class ScoreByClusterCassandraMapper
	extends
	AbstractByClusterMapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>> {

    @Override
    protected void map(Map<String, ByteBuffer> keys,
	    Map<String, ByteBuffer> columns, Context context)
	    throws IOException, InterruptedException {

	float score = columns.get("score").getFloat();

	if (score > 0) {
	    int user = keys.get("user").getInt();
	    context.write(new IntPairWritable(getCluster(user), 1),
		    new IntDoubleOrPrefWritable(user, keys.get("movie")
			    .getInt(), score));
	}

    }

}
