package org.scienty.java.util.concurrent.task;

public class TaskResult {
	public static final int STATUS_SUCCESS = 0;
	public static final int STATUS_FAILURE = 1;
	
	public final String key;
	public final int statusCode;
	public final String message;
	public final Object resultObj;
	
	public TaskResult(String key, int statusCode, String message, Object resultObj) {
		this.key = key;
		this.statusCode = statusCode;
		this.message = message;
		this.resultObj = resultObj;
	}

	@Override
	public String toString() {
		return "TaskResult [key=" + key + ", statusCode=" + statusCode
				+ ", message=" + message + ", resultObj=" + resultObj + "]";
	}

}
