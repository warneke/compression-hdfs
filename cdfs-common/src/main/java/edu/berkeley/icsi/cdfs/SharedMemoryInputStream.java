package edu.berkeley.icsi.cdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import edu.berkeley.icsi.cdfs.sharedmem.SharedMemoryConsumer;

final class SharedMemoryInputStream extends InputStream implements Seekable, PositionedReadable {

	private final Path path;

	private final Socket socket;

	private long seek = 0L;

	private boolean headerSent = false;

	private SharedMemoryConsumer smc = null;

	SharedMemoryInputStream(final Socket socket, final Path path) throws IOException {

		this.socket = socket;
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

		System.out.println("Read 1");

		return 0;
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
			header.toOutputStream(this.socket.getOutputStream());
			this.headerSent = true;
		}

		if (this.smc == null) {
			this.smc = new SharedMemoryConsumer(this.socket);
		}

		final ByteBuffer sharedMemBuf;
		try {
			sharedMemBuf = this.smc.lockSharedMemory();
		} catch (EOFException eof) {
			return -1;
		}

		final int dataToRead = Math.min(sharedMemBuf.remaining(), len);
		sharedMemBuf.get(b, off, dataToRead);

		if (!sharedMemBuf.hasRemaining()) {
			this.smc.unlockSharedMemory();
		}

		return dataToRead;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() throws IOException {

		System.out.println("Available");

		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.socket.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void mark(final int readlimit) {

		System.out.println("Mark");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean markSupported() {

		System.out.println("Mark supported");

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() throws IOException {

		System.out.println("Reset");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(final long n) throws IOException {

		System.out.println("Skip");

		return 0;
	}
}
