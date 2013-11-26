package es.udc.fi.dc.irlab.ppc.hcomputation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestH2Mapper {

    private MapDriver<LongWritable, VectorWritable, LongWritable, VectorWritable> mapDriver;

    @Before
    public void setup() {
	mapDriver = new MapDriver<LongWritable, VectorWritable, LongWritable, VectorWritable>();
    }

    @Test
    public void testMap() throws IOException {
	LongWritable inputKey = new LongWritable(1);
	Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
	VectorWritable inputValue = new VectorWritable(inputVector);

	LongWritable outputKey = new LongWritable(1);
	Vector outputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
	VectorWritable outputValue = new VectorWritable(outputVector);

	mapDriver.withMapper(new H2Mapper());
	mapDriver.withInput(inputKey, inputValue);
	mapDriver.withOutput(outputKey, outputValue);

	mapDriver.runTest();
    }

}
