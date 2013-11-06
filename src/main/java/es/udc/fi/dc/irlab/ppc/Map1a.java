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

package es.udc.fi.dc.irlab.ppc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.spectral.common.IntDoublePairWritable;
import org.apache.mahout.common.IntPairWritable;

public class Map1a
	extends
	Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, IntPairWritable, IntDoublePairWritable> {

    @Override
    protected void map(Map<String, ByteBuffer> keys,
	    Map<String, ByteBuffer> columns, Context context)
	    throws IOException, InterruptedException {

	int i = keys.get("movie").getInt();
	int j = keys.get("user").getInt();
	double value = columns.get("score").getFloat();
	context.write(new IntPairWritable(i, 0), new IntDoublePairWritable(j,
		value));
    }
}
