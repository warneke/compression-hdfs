package edu.berkeley.icsi.cdfs.wlgen;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.generic.AbstractStub;
import eu.stratosphere.pact.common.generic.GenericMapper;
import eu.stratosphere.pact.common.stubs.Collector;

public final class MapTask extends AbstractStub implements GenericMapper<FixedByteRecord, FixedByteRecord> {

	private static final Log LOG = LogFactory.getLog(MapTask.class);

	static final String INPUT_OUTPUT_RATIO = "map.io.ratio";

	private IORatioAdapter ioRatioAdapter;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open(final Configuration parameters) {

		final float ioRatio = parameters.getFloat(INPUT_OUTPUT_RATIO, -1.0f);
		if (ioRatio < 0.0f) {
			throw new IllegalStateException("I/O ratio is not set");
		}
		this.ioRatioAdapter = new IORatioAdapter(ioRatio);
		LOG.info("Map task initiated with I/O ratio " + ioRatio);
	}

	@Override
	public void map(FixedByteRecord record, Collector<FixedByteRecord> out) throws Exception {

		this.ioRatioAdapter.collect(record, out);
	}
}
