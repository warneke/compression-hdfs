package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

final class IORatioAdapter {

	private static final Log LOG = LogFactory.getLog(IORatioAdapter.class);

	private final NullWritable nullWritable;

	private final boolean reduceMode;

	private final int ratio;

	private int counter = 0;

	IORatioAdapter(final float ioRatio) {

		this.nullWritable = NullWritable.get();

		if (ioRatio >= 1.0f) {
			this.reduceMode = true;
			this.ratio = Math.round(ioRatio);
		} else {
			this.reduceMode = false;
			this.ratio = Math.round(1.0f / ioRatio);
		}

		LOG.info("Reduce mode is " + (this.reduceMode ? "activated" : "deactivated") + ", ratio is " + this.ratio);
	}

	void collect(final FixedByteRecord record,
			final Mapper<FixedByteRecord, NullWritable, FixedByteRecord, NullWritable>.Context context)
			throws IOException, InterruptedException {

		if (this.reduceMode) {
			if (++this.counter == this.ratio) {
				context.write(record, this.nullWritable);
				this.counter = 0;
			}

		} else {
			for (int i = 0; i < this.ratio; ++i) {
				context.write(record, this.nullWritable);
			}
		}
	}

	void collect(final FixedByteRecord record,
			final Reducer<FixedByteRecord, NullWritable, FixedByteRecord, NullWritable>.Context context)
			throws IOException, InterruptedException {

		if (this.reduceMode) {
			if (++this.counter == this.ratio) {
				context.write(record, this.nullWritable);
				this.counter = 0;
			}

		} else {
			for (int i = 0; i < this.ratio; ++i) {
				context.write(record, this.nullWritable);
			}
		}
	}
}
