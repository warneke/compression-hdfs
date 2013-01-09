package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import edu.berkeley.icsi.cdfs.datanode.ConnectionMode;
import edu.berkeley.icsi.cdfs.datanode.Header;

final class SeekableInputStream extends InputStream implements Seekable, PositionedReadable {

	private final InputStream inputStream;

	private final OutputStream outputStream;

	private final Path path;

	private long seek = 0L;

	private boolean headerSent = false;

	SeekableInputStream(final InputStream inputStream, final OutputStream outputStream, final Path path) {

		this.inputStream = inputStream;
		this.outputStream = outputStream;
		this.path = path;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final long position, final byte[] buffer, final int offset, final int length) throws IOException {

		System.out.println("read 4");

		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFully(final long position, final byte[] buffer, final int offset, final int length)
			throws IOException {

		System.out.println("readFully");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFully(final long position, final byte[] buffer) throws IOException {

		readFully(position, buffer, 0, buffer.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(final long pos) throws IOException {

		if (this.headerSent) {
			throw new IOException("Cannot seek, header already sent");
		}

		System.out.println("Setting seek to " + pos);
		this.seek = pos;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getPos() throws IOException {

		System.out.println("getPos");

		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean seekToNewSource(final long targetPos) throws IOException {

		System.out.println("seekToNewSource");

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {

		return this.inputStream.read();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final byte b[]) throws IOException {

		return read(b, 0, b.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final byte b[], final int off, final int len) throws IOException {

		// If this is the first read attempt, send header first
		if (!this.headerSent) {
			final Header header = new Header(ConnectionMode.READ, this.path, this.seek);
			header.sendHeader(this.outputStream);
			this.headerSent = true;
		}

		return 0;

		// return this.inputStream.read(b, off, len);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() throws IOException {

		return this.inputStream.available();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.inputStream.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void mark(final int readlimit) {

		this.inputStream.mark(readlimit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean markSupported() {

		return this.inputStream.markSupported();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() throws IOException {

		this.inputStream.reset();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(final long n) throws IOException {

		return this.inputStream.skip(n);
	}
}
