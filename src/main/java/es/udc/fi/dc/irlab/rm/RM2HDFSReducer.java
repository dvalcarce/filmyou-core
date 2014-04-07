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

import org.apache.hadoop.io.FloatWritable;
import org.apache.mahout.common.IntPairWritable;

/**
 * Emit &lt;(j, i), A_{i,j}> to HDFS from &lt;k, {|k|} U {(j, sum_i A_{i,j})} U
 * {(i, j, A_{i,j})}>.
 */
public class RM2HDFSReducer extends
	AbstractRM2Reducer<IntPairWritable, FloatWritable> {

    /**
     * Write preference to HDFS SequenceFile<IntPairWritable, FloatWritable>.
     * 
     * @param context
     *            reduce context
     * @param userId
     *            user ID
     * @param itemId
     *            item ID
     * @param score
     *            predicted score
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void writePreference(Context context, int userId, int itemId,
	    double score, int cluster) throws IOException, InterruptedException {

	context.write(new IntPairWritable(userId + 1, itemId + 1),
		new FloatWritable((float) score));

    }

}
