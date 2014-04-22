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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopUtils;

public class TestWComputationMapper {

	private MapDriver<IntWritable, VectorWritable, IntWritable, VectorWritable> mapDriver;

	@Before
	public void setup() {
		mapDriver = new MapDriver<IntWritable, VectorWritable, IntWritable, VectorWritable>();
	}

	@Test
	public void testReducer() throws IOException {
		String baseDirectory = "TestWComputationMapper";
		Configuration conf = new Configuration();

		HadoopUtils.removeData(conf, baseDirectory);
		IntWritable inputKey = new IntWritable(1);
		VectorWritable inputValue = new VectorWritable(new DenseVector(
				new double[] { 1.0, 2.0, 3.0 }));

		IntWritable outputKey = new IntWritable(1);
		Vector outputVector = new DenseVector(new double[] { 3.0, 4.0, 15.0 });

		Path xPath = DataInitialization.createDoubleMatrix(conf,
				new double[][] { { 3.0, 10.0, 20.0 } }, baseDirectory,
				"x-merged", 1);

		Path yPath = DataInitialization.createDoubleMatrix(conf,
				new double[][] { { 1.0, 5.0, 4.0 } }, baseDirectory,
				"y-merged", 1);

		mapDriver.withCacheFile(xPath.toString());
		mapDriver.withCacheFile(yPath.toString());

		mapDriver.withMapper(new WComputationMapper());
		mapDriver.withInput(inputKey, inputValue);
		mapDriver.withOutput(outputKey, new VectorWritable(outputVector));

		mapDriver.runTest();

		HadoopUtils.removeData(conf, baseDirectory);
	}
}
