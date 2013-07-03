package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class PopularBlock implements Writable {

	private int index;

	private int uncompressedLength;

	public PopularBlock(final int index, final int uncompressedLength) {
		this.index = index;
		this.uncompressedLength = uncompressedLength;
	}

	public PopularBlock() {
	}

	public int getIndex() {
		return this.index;
	}

	public int getUncompressedLength() {
		return this.uncompressedLength;
	}

	public int getCompressedLength() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.index);
		out.writeInt(this.uncompressedLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {

		this.index = in.readInt();
		this.uncompressedLength = in.readInt();
	}

}
