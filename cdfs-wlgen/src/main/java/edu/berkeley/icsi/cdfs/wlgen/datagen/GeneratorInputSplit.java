package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public final class GeneratorInputSplit extends InputSplit implements Writable {

	private long fileSize;

	public GeneratorInputSplit(final long fileSize) {
		this.fileSize = fileSize;
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
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {
		arg0.writeLong(this.fileSize);
	}
}