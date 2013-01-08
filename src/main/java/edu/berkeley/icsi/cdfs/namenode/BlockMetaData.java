package edu.berkeley.icsi.cdfs.namenode;

import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final class BlockMetaData implements KryoSerializable {

	private Path hdfsPath;

	private int length;

	private long offset;

	BlockMetaData(final Path hdfsPath, final int length, final long offset) {
		this.hdfsPath = hdfsPath;
		this.length = length;
		this.offset = offset;
	}

	@SuppressWarnings("unused")
	private BlockMetaData() {
		this.hdfsPath = null;
		this.length = 0;
		this.offset = 0L;
	}

	Path getHdfsPath() {
		return this.hdfsPath;
	}

	int getLength() {
		return this.length;
	}

	long getOffset() {
		return this.offset;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		output.writeString(this.hdfsPath.toString());
		output.writeInt(this.length);
		output.writeLong(this.offset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.hdfsPath = new Path(input.readString());
		this.length = input.readInt();
		this.offset = input.readLong();
	}
}
