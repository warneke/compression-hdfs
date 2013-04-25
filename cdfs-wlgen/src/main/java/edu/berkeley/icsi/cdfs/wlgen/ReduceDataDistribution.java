package edu.berkeley.icsi.cdfs.wlgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.berkeley.icsi.cdfs.wlgen.datagen.DataGeneratorOutput;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;

public class ReduceDataDistribution implements DataDistribution {

	private Key[] boundaries;

	public ReduceDataDistribution(final double[] dataDistribution) {

		this.boundaries = new FixedByteRecord[dataDistribution.length];
		for (int i = 0; i < dataDistribution.length; ++i) {
			this.boundaries[i] = toBucketBoundary(dataDistribution[i]);
		}
	}

	public ReduceDataDistribution() {
	}

	private static FixedByteRecord toBucketBoundary(final double val) {

		final FixedByteRecord record = new FixedByteRecord();

		for (int j = 0; j < FixedByteRecord.LENGTH; ++j) {
			record.getBuffer()[j] = '_';
		}
		record.getBuffer()[FixedByteRecord.LENGTH - 1] = '\n';

		long l = (((long) (val * (double) Integer.MAX_VALUE * 2)) & 0xFFFFFFFF);

		for (int j = 0; j < 8; ++j) {
			final int pos = (int) (0x000f & l);
			record.getBuffer()[j] = DataGeneratorOutput.KEY_ALPHABET[pos];
			l = l >> 4;
		}

		reverseKey(record.getBuffer());

		return record;
	}

	private static void reverseKey(final byte[] buf) {

		swap(buf, 0, 7);
		swap(buf, 1, 6);
		swap(buf, 2, 5);
		swap(buf, 3, 4);
	}

	private static void swap(final byte[] buf, final int pos1, final int pos2) {

		final byte tmp = buf[pos1];
		buf[pos1] = buf[pos2];
		buf[pos2] = tmp;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PactRecord getBucketBoundary(final int bucketNum, final int totalNumBuckets) {

		throw new RuntimeException("getBucketBoundary");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Key getBucketBoundary2(final int bucketNum, final int totalNumBuckets) {

		if (this.boundaries.length != (totalNumBuckets - 1)) {
			throw new IllegalStateException("Number of buckets do not match (" + totalNumBuckets + ", expected "
				+ this.boundaries.length + ")");
		}

		return this.boundaries[bucketNum];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.boundaries.length);
		for (int i = 0; i < this.boundaries.length; ++i) {
			this.boundaries[i].write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		final int length = in.readInt();
		this.boundaries = new FixedByteRecord[length];
		for (int i = 0; i < length; ++i) {
			this.boundaries[i] = new FixedByteRecord();
			this.boundaries[i].read(in);
		}
	}
}
