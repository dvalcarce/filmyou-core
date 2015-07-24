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
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopUtils;

public class TestWCMapper {

    private MapDriver<IntWritable, VectorWritable, IntWritable, VectorWritable> mapDriver;

    @Before
    public void setup() {
        mapDriver = new MapDriver<IntWritable, VectorWritable, IntWritable, VectorWritable>();
    }

    @Test
    public void testMap() throws IOException {
        final String baseDirectory = "TestWCMapper";
        final Configuration conf = new Configuration();

        HadoopUtils.removeData(conf, baseDirectory);

        final Matrix matrix = new DenseMatrix(
                new double[][] { { 1.0, 2.0, 3.0 }, { 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } });
        final Path cPath = DataInitialization.createMapNullMatrix(conf, matrix, baseDirectory,
                "C-merged");

        final IntWritable inputKey = new IntWritable(1);
        final Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
        final VectorWritable inputValue = new VectorWritable(inputVector);

        final IntWritable outputKey = new IntWritable(1);
        final Vector outputVector = new DenseVector(new double[] { 30.0, 36.0, 42.0 });
        final VectorWritable outputValue = new VectorWritable(outputVector);

        mapDriver.withMapper(new WCMapper());
        mapDriver.withCacheFile(cPath.toString());

        mapDriver.withInput(inputKey, inputValue);
        mapDriver.withOutput(outputKey, outputValue);
        mapDriver.runTest();

        HadoopUtils.removeData(conf, baseDirectory);
    }

}
