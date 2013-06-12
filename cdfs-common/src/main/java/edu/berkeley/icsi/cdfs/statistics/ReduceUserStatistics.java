package edu.berkeley.icsi.cdfs.statistics;

public final class ReduceUserStatistics extends AbstractUserStatistics {

	public ReduceUserStatistics(final String jobID, final int taskID, final long startTime, final long endTime) {
		super(jobID, taskID, startTime, endTime);
	}

	public ReduceUserStatistics() {
	}
}
