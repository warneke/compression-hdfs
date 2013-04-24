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

	public void toOutputStream(final OutputStream outputStream) throws IOException {

		outputStream.write(this.connectionMode.toByte());
		final byte[] path = this.path.toString().getBytes();
		final byte[] tmp = new byte[8];
		NumberUtils.integerToByteArray(path.length, tmp, 0);
		outputStream.write(tmp, 0, 4);
		outputStream.write(path);
		NumberUtils.longToByteArray(this.pos, tmp, 0);
		outputStream.write(tmp);
		outputStream.flush();
	}

	static Header fromInputStream(final InputStream inputStream) throws IOException {

		final byte[] tmp = new byte[512];
		readFully(inputStream, tmp, 5);

		final ConnectionMode mode = ConnectionMode.toConnectionMode(tmp[0]);
		final int pathLength = NumberUtils.byteArrayToInteger(tmp, 1);
		readFully(inputStream, tmp, pathLength + 8);
		final Path p = new Path(new String(tmp, 0, pathLength));
		final long pos = NumberUtils.byteArrayToLong(tmp, pathLength);

		return new Header(mode, p, pos);
	}

	private static void readFully(final InputStream inputStream, final byte[] buf, final int len)
			throws IOException {

		int bytesRead = 0;
		while (bytesRead < len) {

			final int r = inputStream.read(buf, bytesRead, len - bytesRead);
			if (r < 0) {
				throw new IOException("Unexpected end of input stream");
			}

			bytesRead += r;
		}
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuffer sb;
		if (this.connectionMode == ConnectionMode.READ) {
			sb = new StringBuffer("READ (");
		} else {
			sb = new StringBuffer("WRITE (");
		}

		sb.append(this.path.toString());
		sb.append(", ");
		sb.append(this.pos);
		sb.append(')');

		return sb.toString();
	}
}
