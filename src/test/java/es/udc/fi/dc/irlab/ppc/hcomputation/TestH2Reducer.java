package es.udc.fi.dc.irlab.ppc.hcomputation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestH2Reducer {

    private ReduceDriver<LongWritable, VectorWritable, LongWritable, VectorWritable> reduceDriver;

    @Before
    public void setup() {
	reduceDriver = new ReduceDriver<LongWritable, VectorWritable, LongWritable, VectorWritable>();
    }

    @Test
    public void testReduce() throws IOException {
	LongWritable inputKey = new LongWritable(1);
	List<VectorWritable> inputValues = new ArrayList<VectorWritable>();
	Vector inputVector1 = new DenseVector(new double[] { 2.0, 4.0, 6.0 });
	Vector inputVector2 = new DenseVector(new double[] { 3.0, 6.0, 9.0 });

	inputValues.add(new VectorWritable(inputVector1));
	inputValues.add(new VectorWritable(inputVector2));

	LongWritable outputKey = new LongWritable(1);
	Vector outputVector = new DenseVector(new double[] { 5.0, 10.0, 15.0 });
	VectorWritable outputValue = new VectorWritable(outputVector);

	reduceDriver.withReducer(new H2Reducer());
	reduceDriver.withInput(inputKey, inputValues);
	reduceDriver.withOutput(outputKey, outputValue);

	reduceDriver.runTest();
    }

}
