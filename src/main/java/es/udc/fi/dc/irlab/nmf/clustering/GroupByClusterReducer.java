package es.udc.fi.dc.irlab.nmf.clustering;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.fi.dc.irlab.util.IntSetWritable;

/**
 * Group all users by cluster.
 * 
 * Emit <k, Set<j>> from <k, {j}>.
 * 
 */
public class GroupByClusterReducer extends
	Reducer<IntWritable, IntWritable, IntWritable, IntSetWritable> {

    @Override
    protected void reduce(IntWritable cluster, Iterable<IntWritable> users,
	    Context context) throws IOException, InterruptedException {

	IntSetWritable set = new IntSetWritable(users);

	context.write(cluster, set);

    }
}