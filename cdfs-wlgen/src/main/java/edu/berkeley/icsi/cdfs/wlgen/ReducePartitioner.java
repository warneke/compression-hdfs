package edu.berkeley.icsi.cdfs.wlgen;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public final class ReducePartitioner extends Partitioner<FixedByteRecord, NullWritable> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPartition(final FixedByteRecord arg0, final NullWritable arg1, final int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

}
