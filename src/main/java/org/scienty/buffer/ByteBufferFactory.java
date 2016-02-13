package org.scienty.buffer;

import java.nio.ByteBuffer;

public class ByteBufferFactory {
	private int bufferSize;
	
	public ByteBufferFactory(int bufferSize) {
		this.bufferSize = bufferSize;
	}
	
	public ByteBuffer acquire() {
		return ByteBuffer.allocate(bufferSize);
	}
	
	public void release(ByteBuffer buff) {
		//TODO: implement pooling 
	}

}
