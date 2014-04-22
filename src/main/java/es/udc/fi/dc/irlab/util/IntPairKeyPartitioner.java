package es.udc.fi.dc.irlab.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.mahout.common.IntPairWritable;

/**
 * Partition the data set according to the first element of the (int, int) key.
 */
public class IntPairKeyPartitioner extends
		Partitioner<IntPairWritable, Writable> {

	@Override
	public int getPartition(IntPairWritable key, Writable value,
			int numPartitions) {

		return (Integer.valueOf(key.getFirst()).hashCode() & Integer.MAX_VALUE)
				% numPartitions;

	}

}
