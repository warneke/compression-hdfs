package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.List;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.CompressedBufferCache;
import edu.berkeley.icsi.cdfs.cache.UncompressedBufferCache;

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
				final WriteOp wo = new WriteOp(header.getPath(), 32 * 1024 * 1024);
				wo.write(inputStream);
			} else {

				final Path path = header.getPath();
				List<Buffer> buffers = UncompressedBufferCache.get().lock(path);
				AbstractReadOp ro = null;
				if (buffers != null) {
					ro = new UncompressedCachedReadOp(buffers);
				} else {
					buffers = CompressedBufferCache.get().lock(path);
					if (buffers != null) {
						ro = new CompressedCachedReadOp(buffers);
					} else {
						ro = new CachingReadOp(header.getPath());
					}
				}

				ro.read(this.socket.getOutputStream());
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
