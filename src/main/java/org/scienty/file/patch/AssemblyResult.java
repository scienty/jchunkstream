package org.scienty.file.patch;


public class AssemblyResult {
	public RangeStore rangeStore;
	//total number of chunks offered for processing
	public int inCount = 0;
	//number of chunks processed successfully
	public int outCount = 0;
	//Last exception that lead to failure
	public Exception ex = null;
	
	AssemblyResult(RangeStore rangeStore, int inCount, int outCount, Exception ex) {
		this.rangeStore = rangeStore;
		this.inCount = inCount;
		this.outCount = outCount;
		this.ex = ex;
	}
	
	public RangeStore getRangeStore() {
		return this.rangeStore;
	}
	
	public int getInCount() {
		return inCount;
	}

	public int getOutCount() {
		return outCount;
	}

	public Exception getEx() {
		return ex;
	}

	@Override
	public String toString() {
		String exMesg = (ex != null ) ? ", ex=" + ex.getMessage() : "";
		return "AssemblyResult [range=" + rangeStore.getRanges()
				+ ", inCount=" + inCount + ", outCount=" + outCount + exMesg + "]";
	}
	
	
};
