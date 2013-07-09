package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class PopularBlock implements Writable {

	private int index;

	private int uncompressedLength;

	private int compressedLength;

	public PopularBlock(final int index, final int uncompressedLength, final int compressedLength) {
		this.index = index;
		this.uncompressedLength = uncompressedLength;
		this.compressedLength = compressedLength;
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
		return this.compressedLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.index);
		out.writeInt(this.uncompressedLength);
		out.writeInt(this.compressedLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {

		this.index = in.readInt();
		this.uncompressedLength = in.readInt();
		this.compressedLength = in.readInt();
	}
}
