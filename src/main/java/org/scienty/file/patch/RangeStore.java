package org.scienty.file.patch;

import java.io.IOException;
import java.nio.channels.FileChannel;


public interface RangeStore {
	/** return ordered list of ByteRange objects **/
	public String getRanges();
	public void add(long low, long high);
	public void sub(long low, long high);
	
	//public void logAdd(long low, long high);
	//public void logSub(long low, long high);
	
	public void load(FileChannel channel) throws IOException;
	public long store(FileChannel channel) throws IOException;
	
	//public void merge();
	public Range span();
	public int size();
}
