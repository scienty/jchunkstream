package org.scienty.file.patch;

public class Range {
	public long low;
	public long high;
	
	public Range (long low, long high) {
		this.low = low;
		this.high = high;
	}
	
	@Override
	public String toString() {
		return low + "-" + high;
	}
}
