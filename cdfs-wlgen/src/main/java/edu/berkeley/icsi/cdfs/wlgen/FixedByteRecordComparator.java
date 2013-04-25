package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.pact.common.generic.types.TypeComparator;

public final class FixedByteRecordComparator implements TypeComparator<FixedByteRecord> {

	private final FixedByteRecord temp1, temp2;

	private FixedByteRecord ref = null;

	public FixedByteRecordComparator() {
		this.temp1 = new FixedByteRecord();
		this.temp2 = new FixedByteRecord();
	}

	@Override
	public int hash(FixedByteRecord record) {

		System.out.println("HASH");

		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setReference(final FixedByteRecord toCompare) {
		this.ref = toCompare;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equalToReference(FixedByteRecord candidate) {

		return (this.ref.compareTo(candidate) == 0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareToReference(final TypeComparator<FixedByteRecord> referencedComparator) {

		final FixedByteRecordComparator refComp = (FixedByteRecordComparator) referencedComparator;

		return this.ref.compareTo(refComp.ref);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compare(final DataInputView firstSource, final DataInputView secondSource) throws IOException {

		this.temp1.read(firstSource);
		this.temp2.read(secondSource);

		return this.temp1.compareTo(this.temp2);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNormalizeKeyLen() {
		return FixedByteRecord.KEY_LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isNormalizedKeyPrefixOnly(final int keyBytes) {

		return (keyBytes >= FixedByteRecord.KEY_LENGTH);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void putNormalizedKey(final FixedByteRecord record, final byte[] target, final int offset, final int numBytes) {

		record.putNormalizedKey(target, offset, numBytes);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TypeComparator<FixedByteRecord> duplicate() {

		return new FixedByteRecordComparator();
	}

}
