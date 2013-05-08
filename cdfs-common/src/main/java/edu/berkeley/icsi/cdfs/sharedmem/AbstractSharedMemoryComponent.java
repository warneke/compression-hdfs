package edu.berkeley.icsi.cdfs.sharedmem;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

public abstract class AbstractSharedMemoryComponent implements Closeable {

	protected static final byte ACK_BYTE = 21;

	private final Socket socket;

	protected final InputStream inputStream;

	protected final OutputStream outputStream;

	protected AbstractSharedMemoryComponent(final Socket socket) throws IOException {
		this.socket = socket;
		this.inputStream = socket.getInputStream();
		this.outputStream = socket.getOutputStream();
	}

	protected void readFully(final byte[] buf, final int len) throws IOException {

		int bytesRead = 0;
		while (bytesRead < len) {

			try {
				final int r = this.inputStream.read(buf, bytesRead, len - bytesRead);
				if (r < 0) {
					if (bytesRead == 0) {
						throw new EOFException();
					} else {
						throw new IOException("Unexpected end of stream");
					}
				}

				bytesRead += r;
			} catch (SocketException se) {
				if (bytesRead == 0) {
					throw new EOFException();
				} else {
					throw se;
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		this.socket.close();
	}
}
