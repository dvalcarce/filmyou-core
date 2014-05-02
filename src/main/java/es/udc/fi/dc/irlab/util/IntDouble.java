package es.udc.fi.dc.irlab.util;

public class IntDouble implements Comparable<IntDouble> {

	private int key;
	private double value;

	public IntDouble(final int key, final double value) {
		this.key = key;
		this.value = value;
	}

	/**
	 * Key getter
	 * 
	 * @return the key
	 */
	public int getKey() {
		return key;
	}

	/**
	 * Value getter
	 * 
	 * @return the value
	 */
	public double getValue() {
		return value;
	}

	@Override
	public int compareTo(final IntDouble other) {
		return Double.compare(other.value, this.value);
	}

	@Override
	public String toString() {
		return String.format("(%d, %f)", key, value);
	}

	@Override
	public int hashCode() {
		return Integer.valueOf(key).hashCode();
	}

	@Override
	public boolean equals(final Object other) {
		if (other == null) {
			return false;
		}
		if (other == this) {
			return true;
		}
		if (!(other instanceof IntDouble)) {
			return false;
		}
		return ((IntDouble) other).key == key;
	}

}
