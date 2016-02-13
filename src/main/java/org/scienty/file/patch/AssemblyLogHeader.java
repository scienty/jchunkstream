package org.scienty.file.patch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;

public class AssemblyLogHeader {
	private final static Charset charset = Charset.defaultCharset();
	private final static int TAG_MAX_BYTES = 16;
	private final static int TAG_SLOT_BYTES = Integer.BYTES + TAG_MAX_BYTES; // actualSize (int)  + md5 is 16bytes
	private final static int RANGE_BYTES = 2*Long.BYTES;
	
	private static byte[] magic = "CFS".getBytes(charset);
	private ByteBuffer spanBuff = null; //expected range 2*8
	private ByteBuffer tagBuff = null;
	private ByteBuffer byteBuff = null;
	
	public AssemblyLogHeader() {
		byteBuff = ByteBuffer.allocate(size());
		byteBuff.put(magic);
		byteBuff.limit(byteBuff.position() + RANGE_BYTES);
		spanBuff = byteBuff.slice();
		byteBuff.putLong(0L); //low
		byteBuff.putLong(0L); //high
		byteBuff.limit(byteBuff.position() + TAG_SLOT_BYTES);
		tagBuff = byteBuff.slice();
		byteBuff.putInt(0); //size of actual tag
		
	}
	
	public void init(Long low, Long high, String tag) {
		if ( low > high ) throw new IllegalArgumentException("Low > high");
		if ( tag.length() > 16) throw new IllegalArgumentException("Tag size limit exceeded");
		spanBuff.clear();
		spanBuff.putLong(low);
		spanBuff.putLong(high);
		tagBuff.clear();
		byte[] tagBytes = tag.getBytes(charset);
		tagBuff.putInt(tagBytes.length);
		tagBuff.put(tagBytes);
	}

	public int size() {
		return magic.length + RANGE_BYTES + TAG_SLOT_BYTES;
	}
	
	public Range span(){
		spanBuff.clear();
		return new Range(spanBuff.getLong(), spanBuff.getLong());
	}
	
	public String tag() {
		tagBuff.clear();
		int tagSize = tagBuff.getInt();
		if ( tagSize > TAG_MAX_BYTES) {
			throw new IllegalArgumentException("Illegal tag size");
		}
		byte[] tagBytes = new byte[tagSize];
		tagBuff.get(tagBytes);
		return new String(tagBytes, charset);
	}
	
	public void write(FileChannel channel) throws IOException {
		byteBuff.clear();
		int n = 0;
		while ( n < size() ) {
			 n += channel.write(byteBuff);
		}
	}
	
	public void read(FileChannel channel) throws IOException {
		if ( (channel.size() - channel.position()) < size() ) {
			throw new IllegalArgumentException("Channel underflow");
		}
		byteBuff.clear();
		int n = 0;
		while (n < size() )
		 n+= channel.read(byteBuff);
		
		for (int i=0; i < magic.length; i++) {
			if ( magic[i] != byteBuff.get(i) )
				throw new IllegalArgumentException("Invalid file signature");
		}
	}
	
	@Override
	public String toString() {
		return "AssemblyLogHeader [getRange()=" + span() + ", getTag()="
				+ tag() + "]";
	}
	
	public static void main (String args[]) throws IOException {
		File file = new File("C:/temp1/head.bin");
		AssemblyLogHeader head = new AssemblyLogHeader();
		System.out.println("Size " + head.size());
		System.out.println("Tag " + head.tag());
		head.init(10L, 20L, "HelloTag91113150");
		FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
		System.out.println("Range " + head.span());
		System.out.println("Tag " + head.tag());
		try {
			head.read(channel);
		} catch (IllegalArgumentException ex) {
			ex.printStackTrace();
		}
		channel.position(0);
		head.write(channel);
		channel.position(0);
		head.read(channel);
		
	}
}
