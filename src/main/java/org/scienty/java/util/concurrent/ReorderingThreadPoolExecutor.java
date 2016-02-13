package org.scienty.java.util.concurrent;

import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReorderingThreadPoolExecutor<K> extends ThreadPoolExecutor {
	private final Logger logger = LogManager.getLogger();

	private final BlockingQueue<Runnable> mQueueRef;
	private final ConcurrentHashMap<K, KeyHoldingFutureTask<K, ?>> mRunnablesMap;
	private final ReentrantReadWriteLock mMapLock;

	/**
	 * Creates a {@link ReorderingThreadPoolExecutor}.
	 * 
	 * The passed {@link BlockingQueue} should be a {@link LinkedBlockingDeque}.
	 * Use the static method {@link #createBlockingQueue()} to retrieve a
	 * compatible queue depending on the API level.
	 * 
	 * @see {@link ThreadPoolExecutor#ThreadPoolExecutor(int, int, long, TimeUnit, BlockingQueue, ThreadFactory)}
	 */
	// superclass constructor
	public ReorderingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
			TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		mQueueRef = workQueue;
		mRunnablesMap = new ConcurrentHashMap<K, KeyHoldingFutureTask<K, ?>>(maximumPoolSize,
				0.75f, corePoolSize);
		mMapLock = new ReentrantReadWriteLock();
	}

	
	public <T> Future<T> submitWithKey(K key, Callable<T> callable) {
		final KeyHoldingFutureTask<K, T> runnable = new KeyHoldingFutureTask<K, T>(key,
				callable);
		mMapLock.readLock().lock(); // read lock
		try {
			mRunnablesMap.put(key, runnable); // O(1)
		} finally {
			mMapLock.readLock().unlock();
		}
		execute(runnable);
		return runnable;
	}

	public void moveToFront(K key) {
		final Runnable runnable;
		mMapLock.readLock().lock(); // read lock
		try {
			runnable = mRunnablesMap.get(key); // O(1)
		} finally {
			mMapLock.readLock().unlock();
		}
		if (runnable != null) {
			if (mQueueRef instanceof LinkedBlockingDeque) {
				final LinkedBlockingDeque<Runnable> blockingDeque = (LinkedBlockingDeque<Runnable>) mQueueRef;
				/*
				 * the Runnable is removed from the executor queue so it's
				 * safe to add it back: we don't risk double running it.
				 * removeLastOccurrence() has linear complexity, however we
				 * assume that the advantages of bringing the runnable on
				 * top of the queue overtake this drawback in a reasonably
				 * small queue.
				 */
				if (blockingDeque.removeLastOccurrence(runnable)) { // O(n)
					blockingDeque.offerFirst(runnable); // O(1)
					logger.debug("Set task highest priority: " + key);
				}
			}
		}
	}

	public void clearKeysMap() {
		logger.debug("Clearing runnables key map, contains " + mRunnablesMap.size() + " keys");
		
		mMapLock.writeLock().lock();
		try {
			// we add a write lock here as we don't want other threads to add
			// tasks here while the map is cleared
			mRunnablesMap.clear();
		} finally {
			mMapLock.writeLock().unlock();
		}
	}
	
	public void cancelAll(boolean tryToForce) {
		for ( KeyHoldingFutureTask<K, ?> future : mRunnablesMap.values() ) {
			future.cancel(tryToForce);
		}
	}
	
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		if (r instanceof KeyHoldingFutureTask) {
			mMapLock.readLock().lock(); // read lock
			try {
				mRunnablesMap.remove(((KeyHoldingFutureTask<?, ?>) r).key, r);
			} finally {
				mMapLock.readLock().unlock();
			}
		}
	}
	
	@Override
	public void purge() {
		mMapLock.writeLock().lock(); // write lock
		try {
			// remove cancelled runnables from the keys map
			final Collection<KeyHoldingFutureTask<K, ?>> runnables = mRunnablesMap.values();
			for (KeyHoldingFutureTask<K, ?> r : runnables) {
				if (r.isCancelled()) {
					runnables.remove(r); // supposedly O(1)
				}
			}
		} finally {
			mMapLock.writeLock().unlock();
		}
		super.purge();
	}
	
	protected void terminated() {
		clearKeysMap(); // clear keys map when terminated
		super.terminated();
	}

	/**
	 * Factory method that creates a {@link LinkedBlockingDeque}, if the API
	 * level is >= 9, or falls back to a {@link LinkedBlockingQueue}.
	 * TODO: make sure this class uses dequeue offerings for efficienty
	 */
	public static BlockingDeque<Runnable> createBlockingQueue() {
		return new LinkedBlockingDeque<Runnable>();
	}
	
	/**
	 * Extension of {@link FutureTask} which just allows setting a key
	 */
	private static class KeyHoldingFutureTask<K, V> extends FutureTask<V> {
		public final K key;
		public KeyHoldingFutureTask(K key, Callable<V> callable) {
			super(callable);
			this.key = key;
		}
	}

	public boolean containsKey(K key) {
		return mRunnablesMap.containsKey(key);
	}
}
