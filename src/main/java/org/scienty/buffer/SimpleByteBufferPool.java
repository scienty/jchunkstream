package org.scienty.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleByteBufferPool {
	private final static Logger logger = LogManager.getLogger();
	public final static int BUFFER_LIST_ARRAY_SIZE = 32;
	final List<ByteBuffer>[] potBuffers;

	public SimpleByteBufferPool() {
		potBuffers= (List<ByteBuffer>[]) new List[32];
		for (int i = 0; i < potBuffers.length; i++) {
			potBuffers[i] = new ArrayList<ByteBuffer>();
		}
	}

	public ByteBuffer aquire(int bytes) {
		int alloc = allocSize(bytes);
		int index = Integer.numberOfTrailingZeros(alloc);
		List<ByteBuffer> list = potBuffers[index];

		ByteBuffer bb = list.isEmpty() ? create(alloc) : list.remove(list.size() - 1);
		bb.position(0).limit(bytes);

		// fill with zeroes to ensure deterministic behavior upon handling 'uninitialized' data
		for (int i = 0, n = bb.remaining(); i < n; i++) {
			bb.put(i, (byte) 0);
		}

		return bb;
	}

	public void release(ByteBuffer buffer) {
		int alloc = allocSize(buffer.capacity());
		if (buffer.capacity() != alloc) {
			throw new IllegalArgumentException("buffer capacity not a power of two");
		}
		
		int index = Integer.numberOfTrailingZeros(alloc);
		potBuffers[index].add(buffer);
	}

	public void flush() {
		for (int i = 0; i < potBuffers.length; i++) {
			potBuffers[i].clear();
		}
	}

	private static int LARGE_SIZE  = 1024 * 1024;
	private ByteBuffer largeBuffer = malloc(LARGE_SIZE);

	private ByteBuffer create(int bytes) {
		if (bytes > LARGE_SIZE)
			return malloc(bytes);

		if (bytes > largeBuffer.remaining()) {
			largeBuffer = malloc(LARGE_SIZE);
		}

		largeBuffer.limit(largeBuffer.position() + bytes);
		ByteBuffer bb = largeBuffer.slice();
		largeBuffer.position(largeBuffer.limit());      
		return bb;
	}

	private static ByteBuffer malloc(int bytes) {
		logger.debug("Allocating new buffer of size " + bytes);
		return ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder());
	}

	private static int allocSize(int bytes) {
		if (bytes <= 0) {
			throw new IllegalArgumentException("attempted to allocate zero bytes");
		}
		return (bytes > 1) ? Integer.highestOneBit(bytes - 1) << 1 : 1;
	}
	
	public static SimpleByteBufferPool synced(final Object mutex) {
		
	      if (mutex == null) {
	         throw new NullPointerException();
	      }

	      return new SimpleByteBufferPool() {
	         @Override
	         public ByteBuffer aquire(int bytes) {
	            synchronized (mutex) {
	               return super.aquire(bytes);
	            }
	         }
	         
	         @Override
	         public void release(ByteBuffer buffer) {
	            synchronized (mutex) {
	               super.release(buffer);
	            }
	         }
	         
	         @Override
	         public void flush() {
	            synchronized (mutex) {
	               super.flush();
	            }
	         }
	      };
	   }
	
	public static void main(String args[]) {
		SimpleByteBufferPool pool = new SimpleByteBufferPool();
		ByteBuffer buff1 = pool.aquire(1024);
		pool.aquire(1024*1024-1024);
		pool.release(buff1);
		buff1 = pool.aquire(1024);
		
	}
}