package edu.berkeley.icsi.cdfs.statistics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;

final class MapReduceJob implements Comparable<MapReduceJob> {

	private static final int BAR_WIDTH = 200;

	private final String jobID;

	private final Path inputFile;

	private final Map<Integer, MapTask> mapTasks = new HashMap<Integer, MapTask>();

	private final Map<Integer, ReduceTask> reduceTasks = new HashMap<Integer, ReduceTask>();

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

		if (this.mapTasks.put(Integer.valueOf(taskID), new MapTask(taskID, startTime, endTime, blockIndex)) != null) {
			System.err.println("Map collision for job " + this.jobID + ", task ID " + taskID);
		}
	}

	void addReduceTask(final int taskID, final long startTime, final long endTime) {

		if (startTime < this.startTime) {
			this.startTime = startTime;
		}

		if (endTime > this.endTime) {
			this.endTime = endTime;
		}

		if (this.reduceTasks.put(Integer.valueOf(taskID), new ReduceTask(taskID, startTime, endTime)) != null) {
			System.err.println("Reduce collision for job " + this.jobID + ", task ID " + taskID);
		}
	}

	Iterator<MapTask> iterator() {

		return this.mapTasks.values().iterator();
	}

	Path getInputFile() {
		return this.inputFile;
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

		int t = 0;
		while (true) {

			final Integer taskID = Integer.valueOf(t++);

			final MapTask mapTask = this.mapTasks.get(taskID);
			if (mapTask == null) {
				break;
			}

			final ReduceTask reduceTask = this.reduceTasks.get(taskID);
			renderBar(sb, this.startTime, gradient, mapTask, reduceTask);
		}

		return sb.toString();
	}

	private static double computeGradient(final long startTime, final long endTime) {

		return (double) BAR_WIDTH / (double) (endTime - startTime);
	}

	private static void renderBar(final StringBuilder sb, final long startTime, final double gradient,
			final MapTask mapTask, final ReduceTask reduceTask) {

		final int mapStartIndex = (int) Math.rint((double) (mapTask.getStartTime() - startTime) * gradient);
		final int mapEndIndex = (int) Math.rint((double) (mapTask.getEndTime() - startTime) * gradient);
		final char mapChar = mapTask.isCached() ? 'M' : 'm';

		final int reduceStartIndex;
		if (reduceTask != null) {
			reduceStartIndex = (int) Math.rint((double) (reduceTask.getStartTime() - startTime) * gradient);
		} else {
			reduceStartIndex = -1;
		}

		final int reduceEndIndex;
		if (reduceTask != null) {
			reduceEndIndex = (int) Math.rint((double) (reduceTask.getEndTime() - startTime) * gradient);
		} else {
			reduceEndIndex = -1;
		}

		sb.append('\n');
		sb.append('[');

		for (int i = 0; i < BAR_WIDTH; ++i) {

			char fillChar = '_';

			if (i >= mapStartIndex && i <= mapEndIndex) {
				fillChar = mapChar;
			} else if (i >= reduceStartIndex && i <= reduceEndIndex) {
				fillChar = 'r';
			}

			sb.append(fillChar);
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
