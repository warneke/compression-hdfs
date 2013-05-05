package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public final class FixedByteOutputFormat extends OutputFormat<FixedByteRecord, NullWritable> {

	public static final String OUTPUT_PATH = "output.path";

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void checkOutputSpecs(final JobContext arg0) throws IOException, InterruptedException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		return new FixedByteOutputCommitter();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RecordWriter<FixedByteRecord, NullWritable> getRecordWriter(final TaskAttemptContext arg0)
			throws IOException, InterruptedException {

		final Configuration conf = arg0.getConfiguration();
		final String outputPath = conf.get(OUTPUT_PATH);

		final int taskID = arg0.getTaskAttemptID().getTaskID().getId();

		return new FixedByteRecordWriter(new Path(outputPath + "_" + taskID), conf);
	}

}
