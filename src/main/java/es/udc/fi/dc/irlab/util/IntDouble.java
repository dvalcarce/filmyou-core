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
		return Double.compare(o.value, this.value);
	}

}
