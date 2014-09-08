/**
 * Copyright 2014 Daniel Valcarce Silva
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.udc.fi.dc.irlab.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom Writable that stores a pair of a string and an integer.
 *
 */
public class StringIntPairWritable implements
		WritableComparable<StringIntPairWritable> {

	private Text key;
	private IntWritable value;

	public StringIntPairWritable() {
		this.key = new Text();
		this.value = new IntWritable();
	}

	public StringIntPairWritable(String k, int v) {
		this.key = new Text(k);
		this.value = new IntWritable(v);
	}

	public void setKey(String k) {
		this.key = new Text(k);
	}

	public void setValue(int v) {
		this.value = new IntWritable(v);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}

	public String getKey() {
		return key.toString();
	}

	public int getValue() {
		return value.get();
	}

	@Override
	public int compareTo(StringIntPairWritable o) {
		int cmp = key.compareTo(o.key);
		if (cmp != 0) {
			return cmp;
		}
		return value.compareTo(o.value);
	}

	@Override
	public String toString() {
		return key + "\t" + value;
	}

	@Override
	public int hashCode() {
		return key.hashCode() * 163 + value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StringIntPairWritable) {
			StringIntPairWritable tp = (StringIntPairWritable) obj;
			return key.equals(tp.key) && value.equals(tp.value);
		}
		return false;
	}

	public static class KeyPartitioner extends
			Partitioner<StringIntPairWritable, Writable> {

		@Override
		public int getPartition(final StringIntPairWritable key,
				final Writable value, final int numPartitions) {

			return (key.getKey().hashCode() & Integer.MAX_VALUE)
					% numPartitions;

		}
	}

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(StringIntPairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
						b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	public static class KeyComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public KeyComparator() {
			super(StringIntPairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				return TEXT_COMPARATOR
						.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof StringIntPairWritable
					&& b instanceof StringIntPairWritable) {
				return ((StringIntPairWritable) a).key
						.compareTo(((StringIntPairWritable) b).key);
			}
			return super.compare(a, b);
		}
	}

}
