package edu.berkeley.icsi.cdfs.statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;

final class MapReduceJob implements Comparable<MapReduceJob> {

	private static final int BAR_WIDTH = 200;

	private final String jobID;

	private final Path inputFile;

	private final List<MapTask> mapTasks = new ArrayList<MapTask>();

	private long startTime = Long.MAX_VALUE;

	private long endTime = Long.MIN_VALUE;

	MapReduceJob(final String jobID, final Path inputFile) {
		this.jobID = jobID;
		this.inputFile = inputFile;
	}

	void addMapTask(final int taskID, final long startTime, final long endTime, final int blockIndex) {

		if (startTime < this.startTime) {
			this.startTime = startTime;
		}

		if (endTime > this.endTime) {
			this.endTime = endTime;
		}

		this.mapTasks.add(new MapTask(taskID, startTime, endTime, blockIndex));
	}

	Iterator<MapTask> iterator() {

		return this.mapTasks.iterator();
	}

	Path getInputFile() {
		return this.inputFile;
	}

	void addReduceTask(final int taskID, final long startTime,
			final long endTime) {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder(this.jobID);
		sb.append(": ");
		final double gradient = computeGradient(this.startTime, this.endTime);
		final long duration = this.endTime - this.startTime;
		sb.append(duration);

		final Iterator<MapTask> it = this.mapTasks.iterator();
		while (it.hasNext()) {
			renderBar(sb, this.startTime, gradient, it.next());
		}

		return sb.toString();
	}

	private static double computeGradient(final long startTime, final long endTime) {

		return (double) BAR_WIDTH / (double) (endTime - startTime);
	}

	private static void renderBar(final StringBuilder sb, final long startTime, final double gradient,
			final AbstractTask task) {

		final int startIndex = (int) Math.rint((double) (task.getStartTime() - startTime) * gradient);
		final int endIndex = (int) Math.rint((double) (task.getEndTime() - startTime) * gradient);
		final char fillChar;
		if (task.isMap()) {
			final MapTask mt = (MapTask) task;
			fillChar = mt.isCached() ? 'M' : 'm';
		} else {
			fillChar = 'R';
		}

		sb.append('\n');
		sb.append('[');

		for (int i = 0; i < BAR_WIDTH; ++i) {
			if (i >= startIndex && i <= endIndex) {
				sb.append(fillChar);
			} else {
				sb.append('_');
			}
		}

		sb.append(']');

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final MapReduceJob o) {

		return (this.jobID.hashCode() - o.jobID.hashCode());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof MapReduceJob)) {
			return false;
		}

		return compareTo((MapReduceJob) obj) == 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return this.jobID.hashCode();
	}
}
