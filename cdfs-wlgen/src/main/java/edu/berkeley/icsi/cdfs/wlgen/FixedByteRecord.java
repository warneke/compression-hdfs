package edu.berkeley.icsi.cdfs.wlgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

public final class FixedByteRecord implements Key {

	public static final int LENGTH = 100;

	public static final int KEY_LENGTH = 8;

	private final byte[] buf = new byte[LENGTH];

	public void set(final byte[] src, final int offset) {
		System.arraycopy(src, offset, this.buf, 0, LENGTH);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return new String(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.write(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		in.readFully(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final Key o) {

		// System.out.println("Slow compareTo called");

		return compareTo((FixedByteRecord) o);
	}

	public int compareTo(final FixedByteRecord o) {

		for (int i = 0; i < KEY_LENGTH; ++i) {
			final int diff = this.buf[i] - o.buf[i];
			if (diff != 0) {
				return diff;
			}
		}

		return 0;
	}

	public void putNormalizedKey(final byte[] target, final int offset, final int numBytes) {

		System.arraycopy(this.buf, 0, target, offset, numBytes);
	}

	public void copyTo(final FixedByteRecord record) {

		System.arraycopy(this.buf, 0, record.buf, 0, LENGTH);
	}

	public byte[] getBuffer() {
		return this.buf;
	}
}
