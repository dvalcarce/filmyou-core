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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TestItemProbInCollectionMapper {

	private MapDriver<IntWritable, DoubleWritable, IntWritable, DoubleWritable> mapDriver;

	private static final String parameter = "rm.totalSum";

	@Before
	public void setup() {
		mapDriver = new MapDriver<IntWritable, DoubleWritable, IntWritable, DoubleWritable>();
	}

	@Test
	public void testMap() throws IOException {
		IntWritable inputKey = new IntWritable(1);
		DoubleWritable inputValue = new DoubleWritable(10.0);

		IntWritable outputKey = new IntWritable(1);
		DoubleWritable outputValue = new DoubleWritable(5.0);

		Configuration conf = mapDriver.getConfiguration();
		conf.set(parameter, "2.0");

		mapDriver.withMapper(new ItemProbInCollectionMapper());
		mapDriver.withInput(inputKey, inputValue);
		mapDriver.withOutput(outputKey, outputValue);
		mapDriver.runTest();
	}

}
