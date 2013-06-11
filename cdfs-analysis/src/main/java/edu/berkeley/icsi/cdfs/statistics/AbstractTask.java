package edu.berkeley.icsi.cdfs.statistics;

abstract class AbstractTask {

	private final int taskID;

	private final long startTime;

	private final long endTime;

	protected AbstractTask(final int taskID, final long startTime, final long endTime) {
		this.taskID = taskID;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	int getTaskID() {
		return this.taskID;
	}

	long getStartTime() {
		return this.startTime;
	}

	long getEndTime() {
		return this.endTime;
	}

	abstract boolean isMap();
}
