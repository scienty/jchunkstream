/**
 * 
 */
package org.scienty.file.patch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
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
 * This class works in async mode, actual writes are done in background and the write method returns immediately
 * 
 * @author prakasid
 *
 */
public class AsyncFileAssembler implements FileAssembler {
	private final static Logger logger = LogManager.getLogger();
	private AsynchronousFileChannel fileChannel;
	private File file = null;
	protected AtomicInteger inCount = new AtomicInteger(0);
	protected AtomicInteger outCount = new AtomicInteger(0);
	protected AssemblyLog assemblyLog = null;
	private ReentrantLock rangeLock = null;
	private Exception lastEx;

	public AsyncFileAssembler(File file, AssemblyLog assemblyLog) {
		this.file = file;
		this.assemblyLog = assemblyLog;
	}

	public void init() throws IOException {
		logger.debug("Initializing...");
		fileChannel = AsynchronousFileChannel.open(this.file.toPath(), 
				StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		completionHandler = new ChunkCompletionHandler();
		rangeLock = new ReentrantLock();
		assemblyLog.init(false);
	}

	/**
	 * Return a sorted list of successful chunk id till now.
	 * For async writes, this returns the current chunk id that are confirmed
	 * @return
	 * @throws IOException 
	 */
	public AssemblyResult result() throws IOException {
		AssemblyResult result = new AssemblyResult(assemblyLog.read(), inCount.get(), outCount.get(), lastEx);
		lastEx = null;
		return result;
	}

	public void close() throws IOException {
		if ( this.fileChannel != null ) {
			logger.info("Closing channel ");
			this.fileChannel.close();
		}
	}

	public void write(ByteBuffer chunk, long offset) {
		logger.trace("Writing chunk at " + offset);
		inCount.incrementAndGet();

		try {
			Range range = new Range(offset, offset + chunk.remaining());
			fileChannel.write(chunk, offset, range, completionHandler);
		} catch (Exception ex) {
			logger.error("Failed to write chunk at " + offset);
			inCount.decrementAndGet();
			lastEx = ex;
		}
	}

	private CompletionHandler<Integer,Range> completionHandler = null;
	private class ChunkCompletionHandler implements CompletionHandler<Integer, Range>{
		//private SortedSet<Long> successIds = null;
		public ChunkCompletionHandler() {
			//successIds = result;
		}
		@Override
		public void failed(Throwable e, Range range) {
			logger.error("Chunk " + range + " failed with exception:", e);
			incOutCount();
		}
		@Override
		public void completed(Integer result, Range range) {
			logger.info("Chunk " + range + " completed writing");
			try {
				assemblyLog.append(range.low, range.high-1);
			} catch (IOException e) {
				lastEx = e;
				logger.error("Failed to log the range " + range);
			}
			incOutCount();
		}

		private int incOutCount() {
			int count = 0;
			//TODO: use reentrent lock conditon
			synchronized (outCount) {
				count = outCount.incrementAndGet();
				outCount.notifyAll();
			}
			return count;
		}
	};

	boolean waiting = false;
	public void waitForAsync() throws InterruptedException {
		synchronized (outCount) {
			if (waiting) {
				throw new IllegalStateException("Already in waiting state");
			}
			while ( outCount.get() < inCount.get() ) {
				waiting = true;
				try {
					logger.debug("waiting "+ outCount + "/" + inCount );
					outCount.wait();
					logger.trace("done waiting " + outCount + "/" + inCount );
				} finally {
					waiting = false;
				}
			}
		}
	}

	public void flush() throws IOException, InterruptedException {
		logger.debug("Flushing channel");
		waitForAsync();
		fileChannel.force(false);
	}
}
