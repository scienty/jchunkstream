/**
 * 
 */
package org.scienty.java.util.concurrent.task;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Prakash
 * Task that can be used in the threadPoolExecutor to sleep for a given period if there is no work.
 * Note that this task is going to consume one thread even while sleeping not the CPU though
 *
 */
public class SleepingTask<V> extends TaskNotificationProxy implements Callable<V>, Runnable, NotifyingTask {
	private long timeout;
	private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
	public SleepingTask(long timeout, TimeUnit unit) {
		super();
		this.timeout = timeout;
		this.timeUnit = unit;
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		call();

	}

	@Override
	public V call() {
		try {
			Thread.sleep(TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
		} catch (Exception ex) {
			//ignore
		} finally {
			completed(this);
		}
		return null;
	}
	
	@Override
	public String getName() {
		return toString();
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	@Override
	public String toString() {
		return "Sleep [timeout=" + timeout + " " + timeUnit
				+ "]";
	}
}
