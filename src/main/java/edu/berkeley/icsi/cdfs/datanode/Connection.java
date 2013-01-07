package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Compressor;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class Connection extends Thread {

	private final Socket socket;

	Connection(final Socket socket) {
		super("DataNodeConnection from " + socket.getRemoteSocketAddress());

		this.socket = socket;
		start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// Read header first
		try {
			final InputStream inputStream = this.socket.getInputStream();

			final Header header = Header.receiveHeader(inputStream);
			if (header == null) {
				throw new IOException("Unexpected end of header");
			}

			// Mode
			if (header.getConnectionMode() == ConnectionMode.WRITE) {
				final WriteOperation wo = new WriteOperation(header.getPath());
				wo.write(inputStream);
			} else {
				System.out.println("READ");
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (this.socket != null) {
				try {
					this.socket.close();
				} catch (IOException ioe) {
				}
			}
		}
	}
}
