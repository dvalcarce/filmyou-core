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

package es.udc.fi.dc.irlab.nmf.ppc.hcomputation;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestPPCHComputationReducer {

	private ReduceDriver<IntPairWritable, VectorWritable, IntWritable, VectorWritable> reduceDriver;

	@Before
	public void setup() {
		reduceDriver = new ReduceDriver<IntPairWritable, VectorWritable, IntWritable, VectorWritable>();
	}

	@Test
	public void testReducer() throws IOException {
		double accuracy = 0.0001;

		IntPairWritable inputKey = new IntPairWritable(1, 0);
		List<VectorWritable> inputValues = new ArrayList<VectorWritable>();

		inputValues.add(new VectorWritable(new DenseVector(new double[] { 1.0,
				2.0, 3.0 })));
		inputValues.add(new VectorWritable(new DenseVector(new double[] { 3.0,
				10.0, 20.0 })));
		inputValues.add(new VectorWritable(new DenseVector(new double[] { 1.0,
				5.0, 4.0 })));

		IntWritable outputKey = new IntWritable(1);
		Vector outputVector = new DenseVector(new double[] { 0.30952381, 0.75,
				1.48275862 });

		reduceDriver.withReducer(new PPCHComputationReducer());
		reduceDriver.withInput(inputKey, inputValues);

		List<Pair<IntWritable, VectorWritable>> list = reduceDriver.run();
		Pair<IntWritable, VectorWritable> pair = list.get(0);

		assertEquals(outputKey, pair.getFirst());
		Vector vector = pair.getSecond().get();

		for (int i = 0; i < outputVector.size(); i++) {
			assertEquals(outputVector.get(i), vector.get(i), accuracy);
		}
	}

}
