package edu.berkeley.icsi.cdfs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public final class PathWrapper implements Writable {

	private Path path;

	public PathWrapper(final Path path) {
		this.path = path;
	}

	public PathWrapper() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		final byte[] bytes = this.path.toString().getBytes();
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {

		final byte[] bytes = new byte[in.readInt()];
		in.readFully(bytes);

		this.path = new Path(new String(bytes));
	}

	public Path getPath() {

		return this.path;
	}

}
