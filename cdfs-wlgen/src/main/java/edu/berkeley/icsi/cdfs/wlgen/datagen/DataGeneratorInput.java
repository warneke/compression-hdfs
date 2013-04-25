package edu.berkeley.icsi.cdfs.wlgen.datagen;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.types.StringRecord;

public class DataGeneratorInput extends AbstractGenericInputTask {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		new RecordWriter<StringRecord>(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {
		// Nothing to do here
	}
}
