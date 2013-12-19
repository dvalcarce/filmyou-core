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

package es.udc.fi.dc.irlab.ppc.wcomputation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import es.udc.fi.dc.irlab.ppc.hcomputation.H4Mapper;

public class TestW4Mapper {

    private MapDriver<IntWritable, VectorWritable, IntWritable, VectorWritable> mapDriver;

    @Before
    public void setup() {
	mapDriver = new MapDriver<IntWritable, VectorWritable, IntWritable, VectorWritable>();
    }

    private Path createCacheFile(String dirname, String filename)
	    throws IOException {

	Path dirPath = new Path(dirname);
	Configuration conf = new Configuration();
	Path filePath = new Path(dirname + "/" + filename);

	FileSystem fs = dirPath.getFileSystem(conf);
	fs.mkdirs(dirPath);
	fs.create(filePath);

	Matrix matrix = new DenseMatrix(new double[][] { { 1.0, 2.0, 3.0 },
		{ 4.0, 5.0, 6.0 }, { 7.0, 8.0, 9.0 } });
	NullWritable key = NullWritable.get();
	MatrixWritable value = new MatrixWritable(matrix);

	try (SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
		filePath, key.getClass(), value.getClass())) {

	    writer.append(key, value);

	}

	return dirPath;

    }

    private void cleanCacheFiles(String dirname) throws IOException {
	Path dirPath = new Path(dirname);
	Configuration conf = new Configuration();
	FileSystem fs = dirPath.getFileSystem(conf);

	fs.delete(dirPath, true);
    }

    @Test
    public void testMap() throws IOException {
	String dir = "TestW4Mapper";
	String file = "file0001";

	IntWritable inputKey1 = new IntWritable(1);
	Vector inputVector1 = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
	VectorWritable inputValue1 = new VectorWritable(inputVector1);
	IntWritable inputKey2 = new IntWritable(2);
	Vector inputVector2 = new DenseVector(new double[] { 10.0, 20.0, 30.0 });
	VectorWritable inputValue2 = new VectorWritable(inputVector2);

	IntWritable outputKey1 = new IntWritable(1);
	Vector outputVector1 = new DenseVector(new double[] { 30., 36.0, 42.0 });
	VectorWritable outputValue1 = new VectorWritable(outputVector1);

	IntWritable outputKey2 = new IntWritable(2);
	Vector outputVector2 = new DenseVector(new double[] { 300.0, 360.0,
		420.0 });
	VectorWritable outputValue2 = new VectorWritable(outputVector2);

	Path path = createCacheFile(dir, file);

	mapDriver.withMapper(new H4Mapper());
	mapDriver.withInput(inputKey1, inputValue1);
	mapDriver.withInput(inputKey2, inputValue2);
	mapDriver.withCacheFile(path.toUri());
	mapDriver.withOutput(outputKey1, outputValue1);
	mapDriver.withOutput(outputKey2, outputValue2);
	mapDriver.runTest();

	cleanCacheFiles(dir);
    }

}
