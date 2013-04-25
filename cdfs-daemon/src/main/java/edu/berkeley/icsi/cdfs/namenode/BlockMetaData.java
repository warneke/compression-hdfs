package edu.berkeley.icsi.cdfs.namenode;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final class BlockMetaData implements KryoSerializable {

	private int index;

	private Path hdfsPath;

	private int length;

	private long offset;

	private final Set<String> cachedCompressed = new HashSet<String>();

	private final Set<String> cachedUncompressed = new HashSet<String>();

	BlockMetaData(final int index, final Path hdfsPath, final int length, final long offset) {
		this.index = index;
		this.hdfsPath = hdfsPath;
		this.length = length;
		this.offset = offset;
	}

	@SuppressWarnings("unused")
	private BlockMetaData() {
		this.index = 0;
		this.hdfsPath = null;
		this.length = 0;
		this.offset = 0L;
	}

	int getIndex() {
		return this.index;
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

	void addCachedBlock(final String host, final boolean compressed) {

		if (compressed) {
			this.cachedCompressed.add(host);
		} else {
			this.cachedUncompressed.add(host);
		}
	}

	void removeCachedBlock(final String host, final boolean compressed) {

		if (compressed) {
			this.cachedCompressed.remove(host);
		} else {
			this.cachedUncompressed.remove(host);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		output.writeInt(this.index);
		output.writeString(this.hdfsPath.toString());
		output.writeInt(this.length);
		output.writeLong(this.offset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.index = input.readInt();
		this.hdfsPath = new Path(input.readString());
		this.length = input.readInt();
		this.offset = input.readLong();
	}
}
