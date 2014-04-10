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

package es.udc.fi.dc.irlab.nmf.wcomputation;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;

/**
 * Emit &lt;(j, 1), (i, A_{i,j})> from HDFS ratings (<(i, j), A_{i,j}>).
 */
public class MovieScoreByUserHDFSMapper
	extends
	Mapper<IntPairWritable, FloatWritable, IntPairWritable, VectorOrPrefWritable> {

    @Override
    protected void map(IntPairWritable keys, FloatWritable columns,
	    Context context) throws IOException, InterruptedException {

	float score = columns.get();

	if (score > 0) {
	    context.write(new IntPairWritable(keys.getFirst(), 1),
		    new VectorOrPrefWritable(keys.getSecond(), score));
	}

    }

}