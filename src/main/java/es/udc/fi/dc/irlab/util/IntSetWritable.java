package es.udc.fi.dc.irlab.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * A Writable version of a Set<Integer>.
 * 
 */
public class IntSetWritable implements Writable {

    private Set<Integer> set = new HashSet<Integer>();

    public IntSetWritable() {

    }

    public IntSetWritable(Iterable<? extends IntWritable> collection) {
	for (IntWritable item : collection) {
	    set.add(item.get());
	}
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	// Clear set
	set.clear();

	// Read size
	int count = in.readInt();

	// Read data
	for (int i = 0; i < count; i++) {
	    set.add(in.readInt());
	}
    }

    @Override
    public void write(DataOutput out) throws IOException {
	// Write size
	out.writeInt(set.size());

	// Write data
	for (int item : set) {
	    out.writeInt(item);
	}
    }

    public Set<Integer> get() {
	return set;
    }

    public void set(Set<Integer> set) {
	this.set = set;
    }

}
