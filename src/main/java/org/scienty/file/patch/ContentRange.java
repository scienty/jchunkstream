package org.scienty.file.patch;

public class ContentRange {
	private Range span;
	private Long size;
	
	public ContentRange(Range span) {
		this(span, null);
	}
	
	public ContentRange(Range span, Long limit) {
		this.span = span;
		this.size = limit;
	}

	public boolean isValid () {
		if (span != null && 
				(span.low > span.high || ( size != null &&  size < span.high )) ) return false;
				
		return true;
	}

	public Range getSpan() {
		return span;
	}

	public void setSpan(Range span) {
		this.span = span;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long limit) {
		this.size = limit;
	}
	
	@Override
	public String toString() {
		return getRangeLimit();
	}

	public String getRange() {
		if ( span == null ) return "*";
		return span.low+ "-" + span.high;
	}
	
	public String getRangeLimit() {
		String limitStr = (size == null ) ? "*" : size.toString();
		return getRange() + "/" + limitStr;
	}
}
