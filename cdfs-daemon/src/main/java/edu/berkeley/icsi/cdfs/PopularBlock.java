package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class PopularBlock implements Writable {

	private int index;

	public PopularBlock(final int index) {
		this.index = index;
	}

	public PopularBlock() {
	}

	public int getIndex() {
		return this.index;
	}

	public int getUncompressedSize() {
		return 0;
	}

	public int getCompressedSize() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {

		this.index = in.readInt();
	}

}
