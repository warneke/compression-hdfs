package edu.berkeley.icsi.cdfs.wlgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public final class FixedByteRecord implements WritableComparable<FixedByteRecord> {

	public static final int LENGTH = 100;

	public static final int KEY_LENGTH = 8;

	private final byte[] buf = new byte[LENGTH];

	public byte[] getData() {
		
		return this.buf;
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
	public void readFields(final DataInput arg0) throws IOException {

		arg0.readFully(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		arg0.write(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final FixedByteRecord o) {

		for (int i = 0; i < KEY_LENGTH; ++i) {
			final int diff = this.buf[i] - o.buf[i];
			if (diff != 0) {
				return diff;
			}
		}

		return 0;
	}
}
