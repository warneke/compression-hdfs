package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public final class PopularFile implements Writable {

	private final PathWrapper path;

	private PopularBlock[] blocks;

	private double popularityFactor;

	public PopularFile(final Path path, final PopularBlock[] blocks, final double popularityFactor) {
		this.path = new PathWrapper(path);
		this.blocks = blocks;
		this.popularityFactor = popularityFactor;
	}

	public PopularFile() {
		this.path = new PathWrapper();
		this.blocks = null;
		this.popularityFactor = -1.0f;
	}

	public Path getPath() {
		return this.path.getPath();
	}

	public double getPopularityFactor() {
		return this.popularityFactor;
	}

	public int getNumberOfBlocks() {
		return this.blocks.length;
	}

	public PopularBlock getBlock(final int index) {
		return this.blocks[index];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.path.write(out);
		out.writeInt(this.blocks.length);
		for (int i = 0; i < this.blocks.length; ++i) {
			this.blocks[i].write(out);
		}

		out.writeDouble(this.popularityFactor);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {

		this.path.readFields(in);
		this.blocks = new PopularBlock[in.readInt()];
		for (int i = 0; i < this.blocks.length; ++i) {
			this.blocks[i] = new PopularBlock();
			this.blocks[i].readFields(in);
		}

		this.popularityFactor = in.readDouble();
	}
}
