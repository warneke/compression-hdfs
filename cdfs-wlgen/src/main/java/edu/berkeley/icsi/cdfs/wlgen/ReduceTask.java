package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceTask extends Reducer<FixedByteRecord, NullWritable, FixedByteRecord, NullWritable> {

	private static final Log LOG = LogFactory.getLog(ReduceTask.class);

	static final String INPUT_OUTPUT_RATIO = "reduce.io.ratio";

	private IORatioAdapter ioRatioAdapter;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setup(final Context context) throws IOException, InterruptedException {

		final Configuration conf = context.getConfiguration();
		final float ioRatio = conf.getFloat(INPUT_OUTPUT_RATIO, -1.0f);
		if (ioRatio < 0.0f) {
			throw new IllegalStateException("I/O ratio is not set");
		}
		this.ioRatioAdapter = new IORatioAdapter(ioRatio);
		LOG.info("Started reducer with I/O ratio " + ioRatio);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reduce(final FixedByteRecord key, final Iterable<NullWritable> values, final Context context)
			throws IOException, InterruptedException {

		System.out.println("REDUCE: " + key);

		int count = 0;
		final Iterator<NullWritable> it = values.iterator();
		while (it.hasNext()) {
			this.ioRatioAdapter.collect(key, context);
			it.next();
			++count;
		}
		System.out.println("Count: " + count);
	}
}
