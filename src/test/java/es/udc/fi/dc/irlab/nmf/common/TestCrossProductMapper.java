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

package es.udc.fi.dc.irlab.nmf.common;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestCrossProductMapper {

    private MapDriver<IntWritable, VectorWritable, NullWritable, MatrixWritable> mapDriver;

    @Before
    public void setup() {
        mapDriver = new MapDriver<IntWritable, VectorWritable, NullWritable, MatrixWritable>();
    }

    @Test
    public void testMap() throws IOException {
        final IntWritable inputKey = new IntWritable(1);
        final Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
        final VectorWritable inputValue = new VectorWritable(inputVector);

        final NullWritable outputKey = NullWritable.get();
        final double[][] outputRows = { { 1.0, 2.0, 3.0 }, { 2.0, 4.0, 6.0 }, { 3.0, 6.0, 9.0 } };

        mapDriver.withMapper(new CrossProductMapper());
        mapDriver.withInput(inputKey, inputValue);

        final List<Pair<NullWritable, MatrixWritable>> list = mapDriver.run();
        final Pair<NullWritable, MatrixWritable> pair = list.get(0);

        assertEquals(outputKey, pair.getFirst());

        int i = 0;
        for (final Vector row : pair.getSecond().get()) {
            assertEquals(new DenseVector(outputRows[i]), row);
            i++;
        }
    }

}
