package org.scienty.file.patch;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface FileAssembler extends Closeable {

	public abstract void init() throws IOException;

	/**
	 * Return AssemblyResult till now with the RangeStore indicating the compated ranges
	 * that are written to file
	 * @return
	 * @throws IOException 
	 */
	public abstract AssemblyResult result() throws IOException;

	public abstract void write(ByteBuffer chunk, long offset);

	public abstract void flush() throws IOException, InterruptedException;

	public abstract void close() throws IOException;

}