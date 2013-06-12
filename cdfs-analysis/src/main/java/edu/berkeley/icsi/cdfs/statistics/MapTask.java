package edu.berkeley.icsi.cdfs.statistics;

final class MapTask extends AbstractTask {

	private final int blockIndex;

	private boolean cached = false;

	MapTask(final int taskID, final long startTime, final long endTime, final int blockIndex) {
		super(taskID, startTime, endTime);

		this.blockIndex = blockIndex;
	}

	int getBlockIndex() {
		return this.blockIndex;
	}

	void markCached() {
		this.cached = true;
	}

	boolean isCached() {
		return this.cached;
	}

	boolean isMap() {
		return true;
	}
}
