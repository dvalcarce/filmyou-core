package es.udc.fi.dc.irlab.baselinerecommender;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import java.io.IOException;

public class BaselineToItemVectorsReducer extends
	Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

    private final VectorWritable merged = new VectorWritable();

    @Override
    protected void reduce(IntWritable row, Iterable<VectorWritable> vectors,
	    Context ctx) throws IOException, InterruptedException {
	merged.setWritesLaxPrecision(true);
	merged.set(VectorWritable.mergeToVector(vectors.iterator()));
	ctx.write(row, merged);
    }
}
