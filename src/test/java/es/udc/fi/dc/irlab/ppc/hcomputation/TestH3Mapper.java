package es.udc.fi.dc.irlab.ppc.hcomputation;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

public class TestH3Mapper {

    private MapDriver<LongWritable, VectorWritable, NullWritable, MatrixWritable> mapDriver;

    @Before
    public void setup() {
	mapDriver = new MapDriver<LongWritable, VectorWritable, NullWritable, MatrixWritable>();
    }

    @Test
    public void testMap() throws IOException {
	LongWritable inputKey = new LongWritable(1);
	Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
	VectorWritable inputValue = new VectorWritable(inputVector);

	NullWritable outputKey = NullWritable.get();
	double[][] outputRows = { { 1.0, 2.0, 3.0 }, { 2.0, 4.0, 6.0 },
		{ 3.0, 6.0, 9.0 } };

	mapDriver.withMapper(new H3Mapper());
	mapDriver.withInput(inputKey, inputValue);

	List<Pair<NullWritable, MatrixWritable>> list = mapDriver.run();
	Pair<NullWritable, MatrixWritable> pair = list.get(0);

	assertEquals(outputKey, pair.getFirst());

	int i = 0;
	for (Vector row : pair.getSecond().get()) {
	    assertEquals(new DenseVector(outputRows[i]), row);
	    i++;
	}
    }

}
