package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public final class FixedByteOutputCommitter extends OutputCommitter {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void abortTask(final TaskAttemptContext arg0) throws IOException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void commitTask(final TaskAttemptContext arg0) throws IOException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean needsTaskCommit(final TaskAttemptContext arg0) throws IOException {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setupJob(final JobContext arg0) throws IOException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setupTask(final TaskAttemptContext arg0) throws IOException {
	}
}
