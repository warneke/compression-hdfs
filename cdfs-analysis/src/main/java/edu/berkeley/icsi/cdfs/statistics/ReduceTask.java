package edu.berkeley.icsi.cdfs.statistics;

final class ReduceTask extends AbstractTask {

	ReduceTask(final int taskID, final long startTime, final long endTime) {
		super(taskID, startTime, endTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	boolean isMap() {
		return false;
	}

}
