package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.DatagramPacket;

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

	public void toPacket(final DatagramPacket packet) throws IOException {

		final byte[] buf = packet.getData();

		buf[0] = this.connectionMode.toByte();
		final byte[] path = this.path.toString().getBytes();
		final int pathLength = path.length;
		NumberUtils.integerToByteArray(pathLength, buf, 1);
		System.arraycopy(path, 0, buf, 5, pathLength);
		NumberUtils.longToByteArray(this.pos, buf, 5 + pathLength);
	}

	static Header fromPacket(final DatagramPacket packet) throws IOException {

		final byte[] buf = packet.getData();

		final ConnectionMode mode = ConnectionMode.toConnectionMode(buf[0]);
		final int pathLength = NumberUtils.byteArrayToInteger(buf, 1);
		final Path p = new Path(new String(buf, 5, pathLength));
		final long pos = NumberUtils.byteArrayToLong(buf, 5 + pathLength);

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
