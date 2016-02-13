package org.scienty.java.util.concurrent.task;

public interface NotifyingTask extends TaskName {
	public void addTaskListener(TaskListener listener);
	public void removeTaskListener(TaskListener listener);
}
