package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

final class SeekableInputStream extends InputStream implements Seekable, PositionedReadable {

	private final InputStream inputStream;

	SeekableInputStream(final InputStream inputStream) {
		this.inputStream = inputStream;
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

		System.out.println("seek");
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

		return this.inputStream.read(b, off, len);
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
