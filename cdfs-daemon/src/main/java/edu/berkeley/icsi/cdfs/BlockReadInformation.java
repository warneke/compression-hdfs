package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class BlockReadInformation implements Writable {

	private int index;

	private long offset;

	private long length;

	private int totalNumberOfBlocks;

	private boolean cacheUncompressed;

	private boolean cacheCompressed;

	public BlockReadInformation(final int index, final long offset, final long length, final int totalNumberOfBlocks,
			final boolean cacheUncompressed, final boolean cacheCompressed) {

		this.index = index;
		this.offset = offset;
		this.length = length;
		this.totalNumberOfBlocks = totalNumberOfBlocks;
		this.cacheUncompressed = cacheUncompressed;
		this.cacheCompressed = cacheCompressed;
	}

	public BlockReadInformation() {
	}

	public int getIndex() {
		return this.index;
	}

	/**
	 * Get the start offset of file associated with this block
	 */
	public long getOffset() {
		return this.offset;
	}

	public long getLength() {
		return this.length;
	}

	public int getTotalNumberOfBlocks() {
		return this.totalNumberOfBlocks;
	}

	public boolean cacheUncompressed() {
		return this.cacheUncompressed;
	}

	public boolean cacheCompressed() {
		return this.cacheCompressed;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.index);
		out.writeLong(this.offset);
		out.writeLong(this.length);
		out.writeInt(this.totalNumberOfBlocks);
		out.writeBoolean(this.cacheUncompressed);
		out.writeBoolean(this.cacheCompressed);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {

		this.index = in.readInt();
		this.offset = in.readLong();
		this.length = in.readLong();
		this.totalNumberOfBlocks = in.readInt();
		this.cacheUncompressed = in.readBoolean();
		this.cacheCompressed = in.readBoolean();
	}

}
