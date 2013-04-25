package edu.berkeley.icsi.cdfs.wlgen;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;

public final class FixedByteRecordFactory implements TypeSerializerFactory<FixedByteRecord>,
		TypeComparatorFactory<FixedByteRecord> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TypeSerializer<FixedByteRecord> getSerializer() {

		return new FixedByteRecordSerializer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<FixedByteRecord> getDataType() {

		return FixedByteRecord.class;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TypeComparator<FixedByteRecord> createComparator(final Configuration config, final ClassLoader cl)
			throws ClassNotFoundException {

		return new FixedByteRecordComparator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TypeComparator<FixedByteRecord> createSecondarySortComparator(final Configuration config,
			final ClassLoader cl) throws ClassNotFoundException {

		return new FixedByteRecordComparator();
	}

}
