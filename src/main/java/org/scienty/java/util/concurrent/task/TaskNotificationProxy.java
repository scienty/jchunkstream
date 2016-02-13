package org.scienty.java.util.concurrent.task;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

public class TaskNotificationProxy implements NotifyingTask, TaskListener {
	private Set<TaskListener> listenerSet = null;
	protected String name = getClass().getSimpleName();

	public TaskNotificationProxy() {
		listenerSet = Collections.newSetFromMap(new WeakHashMap<>(1));
	}

	public TaskNotificationProxy(String name, Set<TaskListener> store) {
		listenerSet = store;
	}

	public Collection<TaskListener> getListeners() {
		return listenerSet;
	}

	public void clearListeners() {
		listenerSet.clear();
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void addTaskListener(TaskListener listener) {
		listenerSet.add(listener);
	}

	@Override
	public void removeTaskListener(TaskListener listener) {
		listenerSet.remove(listener);
	}

	@Override
	public void completed(NotifyingTask notifyingTask) {
		for (TaskListener listener : listenerSet ) {
			listener.completed(notifyingTask);
		}
	}
}
