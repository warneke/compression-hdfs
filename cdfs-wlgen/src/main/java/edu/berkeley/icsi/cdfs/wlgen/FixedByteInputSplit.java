package edu.berkeley.icsi.cdfs.wlgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public final class FixedByteInputSplit extends InputSplit implements Writable {

	private Path path;

	private long offset;

	private long length;

	private String[] locations;

	private boolean isLast;

	public FixedByteInputSplit(final Path path, final long offset, final long length, final String[] locations,
			final boolean isLast) {
		this.path = path;
		this.offset = offset;
		this.length = length;
		this.locations = locations;
		this.isLast = isLast;
	}

	public FixedByteInputSplit() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		this.path = new Path(arg0.readUTF());
		this.offset = arg0.readLong();
		this.length = arg0.readLong();

		final int numberOfLocations = arg0.readInt();
		this.locations = new String[numberOfLocations];
		for (int i = 0; i < numberOfLocations; ++i) {
			this.locations[i] = arg0.readUTF();
		}
		this.isLast = arg0.readBoolean();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		arg0.writeUTF(this.path.toString());
		arg0.writeLong(this.offset);
		arg0.writeLong(this.length);

		arg0.writeInt(this.locations.length);
		for (int i = 0; i < this.locations.length; ++i) {
			arg0.writeUTF(this.locations[i]);
		}
		arg0.writeBoolean(this.isLast);
	}

	public Path getPath() {
		return this.path;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLength() throws IOException, InterruptedException {
		return this.length;
	}

	public long getOffset() {
		return this.offset;
	}

	public boolean isLast() {
		return this.isLast;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return this.locations;
	}
}
