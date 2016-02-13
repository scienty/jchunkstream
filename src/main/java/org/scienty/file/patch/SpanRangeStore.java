package org.scienty.file.patch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

public class SpanRangeStore implements RangeStore {
	private static int sizeIdx = 0;
	private static int lowIdx = 1;
	private static int highIdx = 2;
	private LongBuffer rangeBuff;
	private ByteBuffer byteBuff;
	public SpanRangeStore() {
		init();
	}

	private void init() {
		byteBuff = ByteBuffer.allocateDirect(24);
		rangeBuff = byteBuff.asLongBuffer();
		//rangeBuff.put(sizeIdx, 0L); //store the size
		rangeBuff.put(lowIdx, 0L);
		rangeBuff.put(highIdx, 0L);
		rangeBuff.clear();
	}

	@Override
	public String getRanges() {
		return rangeBuff.get(lowIdx) + "-" +  rangeBuff.get(highIdx);
	}

	@Override
	public void add(long low, long high) {
		if ( low > high ) {
			throw new IllegalArgumentException("low is greater than high");
		}
		long oldLow = rangeBuff.get(lowIdx);
		long oldHigh = rangeBuff.get(highIdx);

		if ( (low < oldLow && high < oldLow -1 ) || 
				high > oldHigh && low > oldHigh + 1) {
			throw new IllegalArgumentException("Range is not contigeous "  + getRanges() + " and " + low + "-" + high);
		}

		rangeBuff.put(sizeIdx, 16L); //store the size
		rangeBuff.put(lowIdx, Math.min(low, oldLow));
		rangeBuff.put(highIdx, Math.max(high, oldHigh));
	}

	@Override
	public void sub(long low, long high) {
		throw new IllegalArgumentException("Not implemented");
	}


	@Override
	public void load(FileChannel channel) throws IOException {
		byteBuff.clear();
		int read = channel.read(byteBuff);
		String errorMesg = null;
		if ( read > 0  && read == 24 &&
				rangeBuff.get(sizeIdx) == 16L) {
			long low = rangeBuff.get(lowIdx);
			long high = rangeBuff.get(highIdx);
			if ( low > high ) {
				errorMesg = "Range low is greater than high";
			}
		} else if ( read != byteBuff.capacity() ) {
			errorMesg = "Insufficient data in channel";
		}

		if ( errorMesg != null ) {
			//reset the buffer
			init();
			throw new IllegalStateException("Channel does not hold expected data");
		}
	}

	@Override
	public long store(FileChannel channel) throws IOException {
		byteBuff.clear();
		return channel.write(byteBuff);
	}

	@Override
	public Range span() {
		if ( size() == 1 )
			return new Range(rangeBuff.get(lowIdx), rangeBuff.get(highIdx));
		return null;
	}

	@Override
	public int size() {
		if ( rangeBuff.get(sizeIdx) == 16L )
			return 1;
		return 0;
	}

}
