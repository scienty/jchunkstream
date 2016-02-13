package org.scienty.java.util.concurrent.task;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class TaskProgress {
	public final Semaphore signal;
	public final long totalWork;
	public final AtomicLong completedWork;
	
	public TaskProgress(Semaphore signal, long totalWork) {
		this.signal = signal;
		this.completedWork = new AtomicLong();
		this.totalWork = totalWork;
	}
	
	public void taskCompleted() {
		signal.release();
	}

	@Override
	public String toString() {
		return "TaskProgress [signal=" + signal + ", completedWork="
				+ completedWork + "]";
	}
}
