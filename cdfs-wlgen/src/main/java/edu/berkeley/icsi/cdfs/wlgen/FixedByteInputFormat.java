package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public final class FixedByteInputFormat implements InputFormat<FixedByteRecord, NullWritable> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RecordReader<FixedByteRecord, NullWritable> getRecordReader(final InputSplit arg0, final JobConf arg1,
			final Reporter arg2) throws IOException {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit[] getSplits(final JobConf arg0, final int arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
