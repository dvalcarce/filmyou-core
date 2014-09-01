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

package es.udc.fi.dc.irlab.rm;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.mahout.common.IntPairWritable;

import es.udc.fi.dc.irlab.common.AbstractByClusterAndCountMapper;
import es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable;
import es.udc.fi.dc.irlab.util.StringIntPairWritable;

/**
 * Emit &lt;(k, 1), (i, j, A_{i,j})> from HDFS ratings (<(i, j), A_{i,j}>) where
 * j is a user from the cluster k.
 */
public class ScoreByClusterHDFSMapper
		extends
		AbstractByClusterAndCountMapper<IntPairWritable, FloatWritable, StringIntPairWritable, IntDoubleOrPrefWritable> {

	@Override
	protected void map(IntPairWritable key, FloatWritable column,
			Context context) throws IOException, InterruptedException {

		float score = column.get();
		if (score > 0) {
			int user = key.getFirst();
			for (String split : getSplits(user)) {
				context.write(new StringIntPairWritable(split, 1),
						new IntDoubleOrPrefWritable(user, key.getSecond(),
								score));
			}

		}

	}
}
