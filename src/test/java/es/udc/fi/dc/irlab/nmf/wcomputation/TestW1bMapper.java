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

package es.udc.fi.dc.irlab.nmf.wcomputation;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestW1bMapper {

    private MapDriver<IntWritable, VectorWritable, IntPairWritable, VectorOrPrefWritable> mapDriver;

    @Before
    public void setup() {
	mapDriver = new MapDriver<IntWritable, VectorWritable, IntPairWritable, VectorOrPrefWritable>();
    }

    @Test
    public void testMapSimpleData() throws IOException, ClassNotFoundException {

	IntWritable inputKey = new IntWritable(1);
	Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
	VectorWritable inputValue = new VectorWritable(inputVector);

	IntPairWritable outputKey = new IntPairWritable(1, 0);
	Vector outputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });

	mapDriver.withMapper(new W1bMapper()).withInput(inputKey, inputValue);

	List<Pair<IntPairWritable, VectorOrPrefWritable>> list = mapDriver
		.run();
	Pair<IntPairWritable, VectorOrPrefWritable> result = list.get(0);
	assertEquals(outputKey, result.getFirst());
	assertEquals(outputVector, result.getSecond().getVector());

    }

}
