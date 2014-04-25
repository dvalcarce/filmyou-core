package es.udc.fi.dc.irlab.util;

public class IntDouble implements Comparable<IntDouble> {

	private int key;
	private double value;

	public IntDouble(int key, double value) {
		this.key = key;
		this.value = value;
	}

	public int getKey() {
		return key;
	}

	public double getValue() {
		return value;
	}

	@Override
	public int compareTo(IntDouble o) {
		int result = Double.compare(o.value, this.value);
		if (result == 0) {
			return Integer.compare(o.key, this.key);
		}
		return result;
	}

	@Override
	public String toString() {
		return String.format("(%d, %f)", key, value);
	}

	@Override
	public int hashCode() {
		return new Integer(key).hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return ((IntDouble) o).key == key;
	}

}
