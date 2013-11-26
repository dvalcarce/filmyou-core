package es.udc.fi.dc.irlab.ppc.hcomputation;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import es.udc.fi.dc.irlab.ppc.hcomputation.H1bMapper;

public class TestH1bMapper {

    private MapDriver<IntWritable, VectorWritable, IntPairWritable, VectorOrPrefWritable> mapDriver;

    @Before
    public void setup() {
	mapDriver = new MapDriver<IntWritable, VectorWritable, IntPairWritable, VectorOrPrefWritable>();
    }

    @Test
    public void testMapSimpleData() throws IOException, ClassNotFoundException {

	IntWritable inputKey = new IntWritable(0);
	Vector inputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });
	VectorWritable inputValue = new VectorWritable(inputVector);

	IntPairWritable outputKey = new IntPairWritable(1, 0);
	Vector outputVector = new DenseVector(new double[] { 1.0, 2.0, 3.0 });

	mapDriver.withMapper(new H1bMapper()).withInput(inputKey, inputValue);

	List<Pair<IntPairWritable, VectorOrPrefWritable>> list = mapDriver
		.run();
	Pair<IntPairWritable, VectorOrPrefWritable> result = list.get(0);
	assertEquals(outputKey, result.getFirst());
	assertEquals(outputVector, result.getSecond().getVector());

    }

}
