package edu.berkeley.icsi.cdfs.wlgen;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.generic.AbstractStub;
import eu.stratosphere.pact.common.generic.GenericReducer;
import eu.stratosphere.pact.common.stubs.Collector;

public class ReduceTask extends AbstractStub implements GenericReducer<FixedByteRecord, FixedByteRecord> {

	private static final Log LOG = LogFactory.getLog(ReduceTask.class);

	static final String INPUT_OUTPUT_RATIO = "reduce.io.ratio";

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
		LOG.info("Reduce task initiated with I/O ratio " + ioRatio);
	}

	@Override
	public void reduce(final Iterator<FixedByteRecord> records, final Collector<FixedByteRecord> out) throws Exception {
		
		while (records.hasNext()) {
			this.ioRatioAdapter.collect(records.next(), out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void combine(final Iterator<FixedByteRecord> records, final Collector<FixedByteRecord> out) throws Exception {

		throw new RuntimeException("combine called");
	}

}
