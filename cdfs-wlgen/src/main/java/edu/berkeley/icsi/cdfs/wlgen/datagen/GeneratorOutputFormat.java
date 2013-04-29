package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.berkeley.icsi.cdfs.wlgen.FixedByteRecord;

public final class GeneratorOutputFormat extends OutputFormat<FixedByteRecord, NullWritable> {

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
	public OutputCommitter getOutputCommitter(final TaskAttemptContext arg0) throws IOException,
			InterruptedException {

		return new GeneratorOutputCommitter();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RecordWriter<FixedByteRecord, NullWritable> getRecordWriter(final TaskAttemptContext arg0)
			throws IOException, InterruptedException {

		final Configuration conf = arg0.getConfiguration();
		final String outputPath = conf.get(DataGenerator.OUTPUT_PATH);

		return new GeneratorRecordWriter(new Path(outputPath), conf);
	}
}