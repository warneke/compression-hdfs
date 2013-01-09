package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final public class Header {

	private final ConnectionMode connectionMode;

	private final Path path;

	private final long pos;

	public Header(final ConnectionMode connectionMode, final Path path, final long pos) {

		this.connectionMode = connectionMode;
		this.path = path;
		this.pos = pos;
	}

	public void sendHeader(final OutputStream outputStream) throws IOException {

		outputStream.write(this.connectionMode.toByte());
		final byte[] size = new byte[8];
		final byte[] path = this.path.toString().getBytes();
		NumberUtils.integerToByteArray(path.length, size, 0);
		outputStream.write(size, 0, 4);
		outputStream.write(path);
		NumberUtils.longToByteArray(this.pos, size, 0);
		outputStream.write(size);
	}

	static Header receiveHeader(final InputStream inputStream) throws IOException {

		final int b = inputStream.read();
		if (b < 0) {
			return null;
		}

		final ConnectionMode mode = ConnectionMode.toConnectionMode((byte) b);
		final byte[] size = new byte[8];
		int r = 0;
		while (r < 4) {
			final int read = inputStream.read(size, r, 4 - r);
			if (read == -1) {
				return null;
			}

			r += read;
		}

		final int pathLength = NumberUtils.byteArrayToInteger(size, 0);
		final byte[] path = new byte[pathLength];
		r = 0;
		while (r < path.length) {
			final int read = inputStream.read(path, r, path.length - r);
			if (read == -1) {
				return null;
			}

			r += read;
		}

		final Path p = new Path(new String(path));

		r = 0;
		while (r < 8) {
			final int read = inputStream.read(size, r, 8 - r);
			if (read == -1) {
				return null;
			}

			r += read;
		}
		final long pos = NumberUtils.byteArrayToLong(size, 0);

		return new Header(mode, p, pos);
	}

	ConnectionMode getConnectionMode() {

		return this.connectionMode;
	}

	Path getPath() {

		return this.path;
	}

	long getPos() {

		return this.pos;
	}
}
