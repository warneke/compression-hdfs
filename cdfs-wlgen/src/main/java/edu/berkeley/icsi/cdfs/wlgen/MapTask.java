package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public final class MapTask extends Mapper<FixedByteRecord, NullWritable, FixedByteRecord, NullWritable> {

	private static final Log LOG = LogFactory.getLog(MapTask.class);

	static final String INPUT_OUTPUT_RATIO = "map.io.ratio";

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
		LOG.info("Started mapper with I/O ratio " + ioRatio);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void map(final FixedByteRecord key, final NullWritable value, final Context context)
			throws IOException, InterruptedException {

		this.ioRatioAdapter.collect(key, context);
	}
}
