package edu.berkeley.icsi.cdfs.statistics;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.protocols.ClientNameNodeProtocol;

public final class StatisticsOutputStream extends OutputStream {

	private static final int BUFFER_SIZE = 21;

	private final ClientNameNodeProtocol nameNode;

	private final String path;

	private final byte[] buf;

	private int numberOfBytesInBuffer = 0;

	public StatisticsOutputStream(final ClientNameNodeProtocol nameNode, final Path path) {

		this.nameNode = nameNode;
		this.path = path.toUri().getPath();
		this.buf = new byte[BUFFER_SIZE];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final int b) throws IOException {

		if (this.numberOfBytesInBuffer == BUFFER_SIZE) {
			throw new IOException("Buffer capacity exhausted");
		}

		this.buf[this.numberOfBytesInBuffer++] = (byte) b;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException {

		if (this.numberOfBytesInBuffer + len > BUFFER_SIZE) {
			throw new IOException("Buffer capacity exhausted");
		}

		System.arraycopy(b, off, this.buf, this.numberOfBytesInBuffer, len);
		this.numberOfBytesInBuffer += len;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		if (this.numberOfBytesInBuffer != BUFFER_SIZE) {
			throw new IOException("Buffer has " + (BUFFER_SIZE - this.numberOfBytesInBuffer) + " empty bytes");
		}

		this.nameNode.reportUserStatistics(new UserStatistics(this.path, this.buf));
	}
}
