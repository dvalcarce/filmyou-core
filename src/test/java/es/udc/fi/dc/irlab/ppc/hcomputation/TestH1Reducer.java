package es.udc.fi.dc.irlab.ppc.hcomputation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestH1Reducer {

    private ReduceDriver<IntPairWritable, VectorOrPrefWritable, LongWritable, VectorWritable> reduceDriver;

    @Before
    public void setup() {
	reduceDriver = new ReduceDriver<IntPairWritable, VectorOrPrefWritable, LongWritable, VectorWritable>();
    }

    @Test
    public void testReduceRightOrder() throws IOException {
	IntPairWritable inputKey = new IntPairWritable(1, 0);
	List<VectorOrPrefWritable> inputValues = new ArrayList<VectorOrPrefWritable>();
	Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });

	inputValues.add(new VectorOrPrefWritable(inputVector));
	inputValues.add(new VectorOrPrefWritable(1, 2.0f));

	LongWritable outputKey = new LongWritable(1);
	Vector outputVector = new DenseVector(new double[] { 2.0, 4.0, 6.0 });

	reduceDriver.withReducer(new H1Reducer()).withInput(inputKey,
		inputValues);
	reduceDriver.withOutput(outputKey, new VectorWritable(outputVector));

	reduceDriver.runTest();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReduceWrongOrder() throws IOException {
	IntPairWritable inputKey = new IntPairWritable(1, 0);
	List<VectorOrPrefWritable> inputValues = new ArrayList<VectorOrPrefWritable>();
	Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });

	inputValues.add(new VectorOrPrefWritable(1, 2.0f));
	inputValues.add(new VectorOrPrefWritable(inputVector));

	reduceDriver.withReducer(new H1Reducer()).withInput(inputKey,
		inputValues);

	reduceDriver.runTest();
    }

}
