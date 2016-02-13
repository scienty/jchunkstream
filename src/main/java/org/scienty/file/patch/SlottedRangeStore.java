package org.scienty.file.patch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

public class SlottedRangeStore implements RangeStore {
	private TreeMap<Long, Long> treeMap;

	public SlottedRangeStore() {
		init();
	}

	private void init() {
		treeMap = new TreeMap<Long, Long>();
		//add(0,0);

	}

	@Override
	public String getRanges() {
		StringBuffer buff = new StringBuffer();
		for (Entry<Long,Long> entry : treeMap.entrySet() ) {
			buff.append(entry.getKey()).append('-').append(entry.getValue()).append(',');
		}
		
		if ( buff.length() > 0 ) {
			//delete last comma
			buff.deleteCharAt(buff.length()-1);
		}
		return buff.toString();
	}

	@Override
	public void add(long low, long high) {
		Long lowToAdd = low;
		Long highToAdd = high;
		
		Entry<Long, Long> entryLELow = treeMap.floorEntry(low);
		Entry<Long, Long> entryLEHigh = treeMap.floorEntry(high+1);
		
		if ( entryLELow != null) {
			Long rangeLELowKey = entryLELow.getKey();
			Long rangeLELowValue = entryLELow.getValue();
			if ( rangeLELowValue >= lowToAdd-1 ) {
				//overlapping, needs to delete
				lowToAdd = rangeLELowKey;
				
				if ( rangeLELowValue >= highToAdd ) {
					highToAdd = rangeLELowValue;
				}
			}
		}
		
		if ( entryLEHigh != null ) {
			Long rangeLEHighValue = entryLEHigh.getValue();
			if ( rangeLEHighValue >= highToAdd ) {
				highToAdd = rangeLEHighValue;
			}
		}

		//System.out.println("AddIn : " + low + "-" + high);
		//System.out.println("Befor : " + getRanges());
		
		//now delete every key between lowToAdd and higToAdd
		treeMap.subMap(lowToAdd, highToAdd+1).clear();
		/*
		while (true) {
			Long key = treeMap.ceilingKey(lowToAdd);
			if ( key != null && key <= highToAdd ) {
				//System.out.println("Remov : " + key);
				treeMap.sub(key);
			} else {
				break;
			}
		}*/
		
		//System.out.println("Add   :" + lowToAdd + "-" + highToAdd);
		treeMap.put(lowToAdd, highToAdd);
		//System.out.println("After : " + getRanges());
		//System.out.println(" ");
	}
	
	@Override
	public void sub(long low, long high) {
		Long lowToRem = low;
		Long highToRem = high;
		
		//System.out.println("RemIn : " + low + "-" + high);
		//adjust entry that is below low
		Entry<Long, Long> entryBelowLow = treeMap.lowerEntry(low);
		if ( entryBelowLow != null) {//there is something lower than low
			Long rangeBelowLowKey = entryBelowLow.getKey();
			Long rangeBelowLowValue = entryBelowLow.getValue();
			if ( rangeBelowLowValue >= lowToRem ) {
				Long oldHigh = rangeBelowLowValue;
				//set higher to low-1
				treeMap.put(rangeBelowLowKey, low-1);
				if ( oldHigh > highToRem ) {
					add(highToRem+1, oldHigh);
				}
			}
		}
		
		//adjust the one that is on higher end
		Entry<Long, Long> entryLEHigh = treeMap.floorEntry(highToRem);
		if ( entryLEHigh != null ) {
			Long rangeLEHighKey = entryLEHigh.getKey();
			Long rangeLEHighValue = entryLEHigh.getValue();
			if ( rangeLEHighKey <= highToRem ) {
				if ( rangeLEHighValue > highToRem ) {
					//sub old key and adjust
					treeMap.remove(entryLEHigh.getKey());
					add(high+1, rangeLEHighValue);
				}
			}
		}
		
		//all entry from low to high
		treeMap.subMap(lowToRem, highToRem+1).clear();
		//System.out.println("After : " + getRanges());
		//System.out.println(" ");
	}

	@Override
	public void load(FileChannel channel) throws IOException {
		ByteBuffer inBuff = ByteBuffer.allocate(8);
		int read = channel.read(inBuff);
		String errorMesg = null;
		if ( read > 0 ) {
			inBuff.flip();
			long rangeLen = inBuff.getLong();
			//new buffer for data
			inBuff = ByteBuffer.allocate((rangeLen < 1024) ? (int)rangeLen : 1024);
			long remaining = rangeLen;
			read = channel.read(inBuff);
			while ( read > 0 && remaining > 0) {
				inBuff.flip();
				while ( inBuff.hasRemaining() ) {
					long low = inBuff.getLong();
					long high = inBuff.getLong();
					add(low, high);
					//?check low > high
				}
				inBuff.clear();
				remaining -= read;
				//dont seek more than required
				if ( remaining > 0 && remaining < inBuff.capacity() ) {
					inBuff.limit((int)remaining);
				}
				read = channel.read(inBuff);

			}

			if ( remaining > 0 ) {
				errorMesg = "Insufficient data in channel";
			}
		}

		if ( errorMesg != null ) {
			//reset the buffer
			init();
			throw new IllegalStateException("Channel does not hold expected data");
		}
	}

	@Override
	public long store(FileChannel channel) throws IOException {
		long rangeLen = treeMap.size() * 2L * 8;
		
		ByteBuffer lenBuff = ByteBuffer.allocate(8);
		lenBuff.putLong(rangeLen);
		lenBuff.flip();
		channel.write(lenBuff);
		ByteBuffer outBuff = ByteBuffer.allocateDirect((rangeLen < 1024) ? (int)rangeLen : 1024);
		
		for (Entry<Long,Long> entry : treeMap.entrySet() ) {
			outBuff.putLong(entry.getKey());
			outBuff.putLong(entry.getValue());
			if ( outBuff.hasRemaining() == false ) {
				channel.write(outBuff);
				outBuff.clear();
			}
		}
		
		return rangeLen;
	}

	@Override
	public Range span() {
		if ( treeMap.size() > 0)
			return new Range(treeMap.firstKey(), treeMap.lastEntry().getValue());
		return null;
	}

	@Override
	public int size() {
		return treeMap.size();
	}
	
	public static void main(String args[]) {
		SlottedRangeStore rs = new SlottedRangeStore();
		rs.add(13L, 13L);
		rs.add(12L, 13L);
		rs.add(10L, 12L);
		rs.add(5L, 7L);
		rs.add(15L, 20L);
		rs.add(16L, 19L);
		
		Random r = new Random();
		for (int i=0; i < 10; i++) {
			Integer rand = r.nextInt(20);
			rs.add(rand, rand+i);
			rand = r.nextInt(20);
			rs.add(rand, rand+i);
			//System.out.println("add " + rand + "-" + (rand+i));
		}
	}
}
