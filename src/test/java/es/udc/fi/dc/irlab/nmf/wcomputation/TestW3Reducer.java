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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.wcomputation.W3Reducer;

public class TestW3Reducer {
    private ReduceDriver<NullWritable, MatrixWritable, NullWritable, MatrixWritable> reduceDriver;

    @Before
    public void setup() {
	reduceDriver = new ReduceDriver<NullWritable, MatrixWritable, NullWritable, MatrixWritable>();
    }

    @Test
    public void testReduceNullWritableIterableOfMatrixWritableContext()
	    throws IOException {
	NullWritable inputKey = NullWritable.get();
	List<MatrixWritable> inputValues = new ArrayList<MatrixWritable>();
	Matrix inputMatrix1 = new DenseMatrix(new double[][] {
		{ 1.0, 2.0, 3.0 }, { 2.0, 4.0, 6.0 }, { 3.0, 6.0, 9.0 } });
	Matrix inputMatrix2 = new DenseMatrix(new double[][] {
		{ 5.0, 5.0, 5.0 }, { 2.0, 2.0, 2.0 }, { 3.0, 4.0, 5.0 } });
	inputValues.add(new MatrixWritable(inputMatrix1));
	inputValues.add(new MatrixWritable(inputMatrix2));

	NullWritable outputKey = NullWritable.get();
	double[][] outputRows = new double[][] { { 6.0, 7.0, 8.0 },
		{ 4.0, 6.0, 8.0 }, { 6.0, 10.0, 14.0 } };

	reduceDriver.withReducer(new W3Reducer());
	reduceDriver.withInput(inputKey, inputValues);

	List<Pair<NullWritable, MatrixWritable>> list = reduceDriver.run();
	Pair<NullWritable, MatrixWritable> pair = list.get(0);

	assertEquals(outputKey, pair.getFirst());

	int i = 0;
	for (Vector row : pair.getSecond().get()) {
	    assertEquals(new DenseVector(outputRows[i]), row);
	    i++;
	}
    }

}
