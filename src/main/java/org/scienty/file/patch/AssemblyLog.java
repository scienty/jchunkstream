package org.scienty.file.patch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class AssemblyLog {
	//low (long), high (long), blank
	private static final int ENTRY_SIZE = 2*Long.BYTES;
	
	private AssemblyLogHeader header;
	private File file;
	private FileChannel wrChannel;
	private FileChannel rdChannel;
	//private ReentrantLock wrLock = null;
	public AssemblyLog(File logfile) {
		this.file = logfile;
		//wrLock = new ReentrantLock();
		header = new AssemblyLogHeader();
	}
	
	/**
	 * Initialize the object with new header fields and file.
	 * Set the provided values to header and create new file if not exist
	 * Load the new file with header and ignore the provided values
	 * @param low
	 * @param high
	 * @param tag
	 * @return true if new file is created with provided parameters, false if the values loaded from existing file
	 * @throws IOException
	 */
	public boolean initFile(Long low, Long high, String tag) throws IOException {
		if ( file.exists() == false  ) {
			header.init(low, high, tag);
			init(false);
			return true;
		}
		
		init(false);
		return false;
	}
	
	public void init(boolean readOnly) throws IOException {
		boolean refreshHeader = true;
		
		if ( readOnly == false && wrChannel == null ) {
			wrChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			if ( wrChannel.size() == 0) {
				//new file
				header.write(wrChannel);
				refreshHeader = false;
			} else if (wrChannel.size() < header.size()) {
				throw new IllegalStateException("Invalid file size");
			}
		} 
		
		if ( rdChannel == null ) {
			rdChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
			if ( refreshHeader ) {
				header.read(rdChannel);
			}
		}
		
	}
	
	public void append(long low, long high) throws IOException {
		//TODO: use buffer pool
		//This is mostly thread safe as each write to the channel is safe
		ByteBuffer writeBuff = ByteBuffer.allocate(ENTRY_SIZE);
		writeBuff.clear();
		writeBuff.putLong(low);
		writeBuff.putLong(high);
		writeBuff.flip();
		while (writeBuff.hasRemaining()) {
			wrChannel.write(writeBuff);
		}
	}
	
	/**
	 * Return the range store with remaining ranges of a file to complete
	 * @return
	 * @throws IOException
	 */
	public RangeStore read() throws IOException {
		//TODO: use buffer pool
		ByteBuffer readBuff = ByteBuffer.allocate(ENTRY_SIZE);
		SlottedRangeStore rangeStore = new SlottedRangeStore();
		Range fullRange = header.span();
		rangeStore.add(fullRange.low, fullRange.high);
		rdChannel.position(header.size());
		
		while ( rdChannel.read(readBuff) == readBuff.capacity() ) {
			readBuff.rewind();
			rangeStore.sub(readBuff.getLong(), readBuff.getLong());
			readBuff.rewind();
		}
		return rangeStore;
	}
	
	public AssemblyLogHeader header() {
		return this.header;
	}
	
	public void flush() throws IOException {
		wrChannel.force(false);
	}
	
	public void close() throws IOException {
		if ( wrChannel != null ) {
			wrChannel.close();
			wrChannel = null;
		}
		
		if ( rdChannel != null ) {
			rdChannel.close();
			rdChannel = null;
		}
	}
	
	/**
	 * This is not a thread safe operation
	 * @throws IOException
	 */
	public void compact() throws IOException {
		throw new IOException ("Not implemented");
		//RangeStore ranges = read();
		//wrChannel.position(header.size());
		//iterate ranges and write to file
		//wrChannel.truncate(wrChannel.position());
	}
	
	public boolean delete() {
		if (file != null && file.exists() && file.isFile() ) return file.delete();
		return false;
	}
	
	public static void main(String args[]) throws IOException {
		AssemblyLog logFile = new AssemblyLog(new File("c:/temp1/testlog.bin"));
		logFile.initFile(0L,1000L, "Taggg");
		System.out.println("Ranges " + logFile.read().getRanges() );
		logFile.flush();
		logFile.close();
		logFile.init(true);
		System.out.println(logFile.header());
		logFile.init(false);
		logFile.append(10, 30);
		logFile.append(50,  90);
		logFile.append(80, 100);
		System.out.println("Ranges " + logFile.read().getRanges() );
		logFile.flush();
		System.out.println("Ranges " + logFile.read().getRanges() );
		logFile.close();
		System.out.println(logFile.header());
	}
}
