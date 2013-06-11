package edu.berkeley.icsi.cdfs.statistics;

final class MapTask extends AbstractTask {

	MapTask(final int taskID, final long startTime, final long endTime) {
		super(taskID, startTime, endTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	boolean isMap() {
		return true;
	}
}
