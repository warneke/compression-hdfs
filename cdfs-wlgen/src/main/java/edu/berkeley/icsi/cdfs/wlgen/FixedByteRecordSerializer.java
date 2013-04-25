package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;

public final class FixedByteRecordSerializer implements TypeSerializer<FixedByteRecord> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FixedByteRecord createInstance() {

		return new FixedByteRecord();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FixedByteRecord createCopy(FixedByteRecord from) {
		System.out.println("createCopy");
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void copyTo(final FixedByteRecord from, final FixedByteRecord to) {

		from.copyTo(to);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getLength() {
		return 100;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long serialize(final FixedByteRecord record, final DataOutputView target) throws IOException {

		record.write(target);

		return FixedByteRecord.LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deserialize(final FixedByteRecord target, final DataInputView source) throws IOException {

		target.read(source);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void copy(final DataInputView source, final DataOutputView target) throws IOException {

		target.write(source, FixedByteRecord.LENGTH);
	}
}
