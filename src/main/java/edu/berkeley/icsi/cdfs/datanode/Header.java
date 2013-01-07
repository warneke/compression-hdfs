package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final public class Header {

	private final ConnectionMode connectionMode;

	private final Path path;

	public Header(final ConnectionMode connectionMode, final Path path) {

		this.connectionMode = connectionMode;
		this.path = path;
	}

	public void sendHeader(final OutputStream outputStream) throws IOException {

		outputStream.write(this.connectionMode.toByte());
		final byte[] size = new byte[4];
		final byte[] path = this.path.toString().getBytes();
		NumberUtils.integerToByteArray(path.length, size, 0);
		outputStream.write(size);
		outputStream.write(path);
	}

	static Header receiveHeader(final InputStream inputStream) throws IOException {

		final int b = inputStream.read();
		if (b < 0) {
			return null;
		}

		final ConnectionMode mode = ConnectionMode.toConnectionMode((byte) b);
		final byte[] size = new byte[4];
		int r = 0;
		while (r < size.length) {
			final int read = inputStream.read(size, r, size.length - r);
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

		return new Header(mode, p);
	}

	ConnectionMode getConnectionMode() {

		return this.connectionMode;
	}

	Path getPath() {

		return this.path;
	}
}
