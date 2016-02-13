package org.scienty.file.patch;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.scienty.buffer.ByteBufferFactory;

public class ChunkOutputStream extends OutputStream {
	private long startPos;
	private FileAssembler assembler;
	private ByteBufferFactory buffFactory;
	private ByteBuffer buff;
	private long reqCount = 0;
	private long inCount = 0;
	ChunkOutputStream(FileAssembler assembler, long startPos, long endPos, ByteBufferFactory bufferFactory) {
		this.assembler = assembler;
		this.startPos = startPos;
		this.buffFactory = bufferFactory;
		if ( endPos < startPos) throw new IllegalArgumentException("startPos > endPos");
		reqCount = endPos - startPos + 1;
	}
	
	@Override
	public void write(int b) throws IOException {
		if ( inCount >= reqCount ) throw new BufferOverflowException();

		inCount ++;
		if (buff == null ) buff = buffFactory.acquire();
		buff.put((byte)b);
		
		if ( buff.remaining() <= 0 ) {
			flush();
		}
	}
	
	//TODO: implement better write method
	
	@Override
	public void flush() {
		if ( buff != null) {
			buff.flip();
			int size = buff.remaining();
			assembler.write(buff, startPos);
			startPos += size;
			buff = null;
		}
	}
	
	@Override
	public void close() {
		flush();
	}
}
