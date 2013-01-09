package edu.berkeley.icsi.cdfs.namenode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final class FileMetaData implements KryoSerializable {

	private Path path;

	private final List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

	private long length;

	private long modificationTime;

	FileMetaData(final Path path) {
		this.path = path;
		this.length = 0L;
		this.modificationTime = System.currentTimeMillis();
	}

	@SuppressWarnings("unused")
	private FileMetaData() {
		this.path = null;
		this.length = 0L;
		this.modificationTime = 0L;
	}

	Path getPath() {
		return this.path;
	}

	BlockMetaData[] getBlockMetaData(final long start, final long len) {

		if (start >= this.length) {
			return null;
		}

		final ArrayList<BlockMetaData> blocks = new ArrayList<BlockMetaData>(this.blocks.size());
		final Iterator<BlockMetaData> it = this.blocks.iterator();

		final long end = start + len;

		while (it.hasNext()) {

			final BlockMetaData bmd = it.next();
			if (overlap(start, end, bmd.getOffset(), bmd.getOffset() + bmd.getLength())) {
				blocks.add(bmd);
			}

			if (bmd.getOffset() > end) {
				break;
			}
		}

		return blocks.toArray(new BlockMetaData[0]);
	}

	private static boolean overlap(final long startA, final long endA, final long startB, final long endB) {

		return (startA < endB && startB < endA);
	}

	void addNewBlock(final Path hdfsPath, final int blockIndex, final int blockLength) {

		// Sanity check
		if (blockIndex != this.blocks.size()) {
			throw new IllegalStateException("Expected block " + this.blocks.size() + ", but received " + blockIndex);
		}

		this.blocks.add(new BlockMetaData(blockIndex, hdfsPath, blockLength, this.length));

		// Increase the length of the total file
		this.length += blockLength;

		// Update modification time
		this.modificationTime = System.currentTimeMillis();
	}

	long getLength() {
		return this.length;
	}

	long getModificationTime() {
		return this.modificationTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		output.writeString(this.path.toString());
		output.writeLong(this.length);
		output.writeLong(this.modificationTime);
		output.writeInt(this.blocks.size());
		for (final BlockMetaData bmd : this.blocks) {
			kryo.writeObject(output, bmd);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.path = new Path(input.readString());
		this.length = input.readLong();
		this.modificationTime = input.readLong();
		final int numberOfBlocks = input.readInt();
		for (int i = 0; i < numberOfBlocks; ++i) {
			this.blocks.add(kryo.readObject(input, BlockMetaData.class));
		}
	}
}
