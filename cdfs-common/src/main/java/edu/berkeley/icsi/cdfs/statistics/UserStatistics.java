package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class UserStatistics implements Writable {

	private String path;

	private byte[] buf;

	public UserStatistics(final String path, final byte[] buf) {
		this.path = path;
		this.buf = buf;
	}

	public UserStatistics() {
		this.path = null;
		this.buf = null;
	}

	public String getPath() {

		return this.path;
	}

	public byte[] getBuffer() {

		return this.buf;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		this.path = arg0.readUTF();
		final int bufferSize = arg0.readInt();
		this.buf = new byte[bufferSize];
		arg0.readFully(this.buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		arg0.writeUTF(this.path);
		arg0.writeInt(this.buf.length);
		arg0.write(this.buf);
	}

}
