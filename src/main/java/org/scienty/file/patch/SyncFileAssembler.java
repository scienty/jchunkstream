/**
 * 
 */
package org.scienty.file.patch;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Assemble list of chunks into a file
 * It is the responsibility of the user to check if use existing file or create new file.
 * 1. use existing partial file in case there is no change is source content
 * 2. use new file, get chunks from old file in case there is change in content
 *  
 * @author prakasid
 *
 */
public class SyncFileAssembler implements Closeable, FileAssembler {
	private final static Logger logger = LogManager.getLogger();
	private FileChannel fileChannel;
	private File file = null;
	protected AtomicInteger inCount = new AtomicInteger(0);
	protected AtomicInteger outCount = new AtomicInteger(0);
	protected AssemblyLog assemblyLog = null;
	private ReentrantLock rangeLock = null;
	private Exception lastEx;

	public SyncFileAssembler(File file, AssemblyLog assemblyLog) throws IOException {
		this.file = file;
		this.assemblyLog = assemblyLog;
	}
	
	/* (non-Javadoc)
	 * @see org.scienty.file.patch.FileAssembler#init()
	 */
	@Override
	public void init() throws IOException {
		logger.debug("Initializing...");
		fileChannel = FileChannel.open(this.file.toPath(), 
				StandardOpenOption.CREATE, StandardOpenOption.WRITE);

		rangeLock = new ReentrantLock();
		assemblyLog.init(false);
	}

	/* (non-Javadoc)
	 * @see org.scienty.file.patch.FileAssembler#result()
	 */
	@Override
	public AssemblyResult result() throws IOException {
		AssemblyResult result = new AssemblyResult(assemblyLog.read(), inCount.get(), outCount.get(), lastEx);
		lastEx = null;
		return result;
	}

	/* (non-Javadoc)
	 * @see org.scienty.file.patch.FileAssembler#write(java.nio.ByteBuffer, long)
	 */
	@Override
	public void write(ByteBuffer chunk, long offset) {
		logger.trace("Writing chunk at " + offset);
		inCount.incrementAndGet();
		try {
			int size = chunk.remaining();
			int wrCount = 0;
			while( wrCount < size) {
				wrCount += fileChannel.write(chunk, offset+wrCount);
			}
			assemblyLog.append(offset, offset + size - 1);
			outCount.incrementAndGet();
		} catch (Exception ex) {
			lastEx = ex;
			logger.error("Failed to write chunk at " + offset);
			inCount.decrementAndGet();
		}
	}

	/* (non-Javadoc)
	 * @see org.scienty.file.patch.FileAssembler#flush()
	 */
	@Override
	public void flush() throws IOException, InterruptedException {
		logger.debug("Flushing channel");
		fileChannel.force(false);
	}

	/* (non-Javadoc)
	 * @see org.scienty.file.patch.FileAssembler#close()
	 */
	@Override
	public void close() throws IOException {
		if ( this.fileChannel != null ) {
			logger.info("Closing channel ");
			this.fileChannel.close();
		}
	}
}
