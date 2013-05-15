package edu.berkeley.icsi.cdfs.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public class EvictionEntry implements Writable {

	private final PathWrapper pathWrapper;

	private int numberOfBlocks;

	private boolean compressed;

	public EvictionEntry(final Path path, final int numberOfBlocks, final boolean compressed) {
		this.pathWrapper = new PathWrapper(path);
		this.numberOfBlocks = numberOfBlocks;
		this.compressed = compressed;
	}

	public EvictionEntry() {
		this.pathWrapper = new PathWrapper();
	}

	public PathWrapper getPathWrapper() {
		return this.pathWrapper;
	}

	public int getNumberOfBlocks() {
		return this.numberOfBlocks;
	}

	public boolean isCompressed() {
		return this.compressed;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		this.pathWrapper.readFields(arg0);
		this.numberOfBlocks = arg0.readInt();
		this.compressed = arg0.readBoolean();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		this.pathWrapper.write(arg0);
		arg0.writeInt(this.numberOfBlocks);
		arg0.writeBoolean(this.compressed);
	}
}
