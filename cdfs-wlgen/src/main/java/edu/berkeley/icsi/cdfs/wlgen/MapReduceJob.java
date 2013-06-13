package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.berkeley.icsi.cdfs.traces.TraceJob;

public final class MapReduceJob extends Job {

	private final int numMapTasks;

	private final String description;

	public MapReduceJob(final Configuration conf, final String jobName, final int numMapTasks) throws IOException {
		this(conf, jobName, numMapTasks, null);
	}

	public MapReduceJob(final Configuration conf, final String jobName, final int numMapTasks, final TraceJob traceJob)
			throws IOException {
		super(conf, jobName);

		this.numMapTasks = numMapTasks;
		if (traceJob != null) {
			this.description = generateDescription(traceJob);
		} else {
			this.description = jobName + " (M[" + numMapTasks + "] -> R[" + getNumReduceTasks() + "])";
		}
	}

	private static String toGB(final long numberOfBytes) {

		double numberOfBytesInGB = ((double) numberOfBytes) / (1024.0 * 1024.0 * 1024.0);

		return String.format("%.3f", numberOfBytesInGB);
	}

	private static String generateDescription(final TraceJob traceJob) {

		final StringBuilder sb = new StringBuilder(traceJob.getJobID());

		sb.append(" (");
		sb.append(toGB(traceJob.getInputFile().getUncompressedFileSize()));
		sb.append(" -> M[");
		sb.append(traceJob.getNumberOfMapTasks());
		sb.append("] -> ");
		sb.append(toGB(traceJob.getSizeOfIntermediateData()));
		sb.append(" -> R[");
		sb.append(traceJob.getNumberOfReduceTasks());
		sb.append("] -> ");
		sb.append(toGB(traceJob.getOutputFile().getUncompressedFileSize()));
		sb.append(')');
		
		return sb.toString();
	}

	public int getNumMapTasks() {
		return this.numMapTasks;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return this.description;
	}
}
