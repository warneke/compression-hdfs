package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.berkeley.icsi.cdfs.wlgen.FixedByteRecord;

public final class GeneratorInputFormat extends InputFormat<FixedByteRecord, NullWritable> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RecordReader<FixedByteRecord, NullWritable> createRecordReader(final InputSplit arg0,
			final TaskAttemptContext arg1) throws IOException, InterruptedException {

		final GeneratorInputSplit gis = (GeneratorInputSplit) arg0;
		return new GeneratorRecordReader(gis.getLength());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<InputSplit> getSplits(final JobContext arg0) throws IOException, InterruptedException {

		final Configuration conf = arg0.getConfiguration();
		final long fileSize = conf.getLong(DataGenerator.FILE_SIZE, -1L);

		final List<InputSplit> list = new ArrayList<InputSplit>(1);
		list.add(new GeneratorInputSplit(fileSize));

		return list;
	}
}