package es.udc.fi.dc.irlab.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the data set according to the value of the integer key.
 */
public class IntKeyPartitioner extends Partitioner<IntWritable, Writable> {

    @Override
    public int getPartition(final IntWritable key, final Writable value, final int numPartitions) {

        return key.get() % numPartitions;

    }

}
