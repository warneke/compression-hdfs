package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public final class GeneratorInputSplit extends InputSplit implements Writable {

	private long fileSize;

	private int compressionFactor;

	public GeneratorInputSplit(final long fileSize, final int compressionFactor) {
		this.fileSize = fileSize;
		this.compressionFactor = compressionFactor;
	}

	public GeneratorInputSplit() {
		this.fileSize = 0L;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLength() throws IOException, InterruptedException {
		return this.fileSize;
	}

	public int getCompressionFactor() {
		return this.compressionFactor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {
		this.fileSize = arg0.readLong();
		this.compressionFactor = arg0.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {
		arg0.writeLong(this.fileSize);
		arg0.writeInt(this.compressionFactor);
	}
}
