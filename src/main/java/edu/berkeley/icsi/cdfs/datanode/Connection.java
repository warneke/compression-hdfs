package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.List;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.CompressedBufferCache;
import edu.berkeley.icsi.cdfs.cache.UncompressedBufferCache;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

final class Connection extends Thread {

	private final Socket socket;

	private final DataNodeNameNodeProtocol nameNode;

	Connection(final Socket socket, final DataNodeNameNodeProtocol nameNode) {
		super("DataNodeConnection from " + socket.getRemoteSocketAddress());

		this.socket = socket;
		this.nameNode = nameNode;
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
				final WriteOp wo = new WriteOp(this.nameNode, header.getPath(), 128 * 1024 * 1024);
				wo.write(inputStream);
			} else {

				CDFSBlockLocation[] blockLocations;
				synchronized (this.nameNode) {
					blockLocations = this.nameNode.getFileBlockLocations(new PathWrapper(header.getPath()),
						header.getPos(), 0L);
				}

				if (blockLocations == null) {
					throw new IllegalStateException("blockLocations is null");
				}

				if (blockLocations.length != 1) {
					throw new IllegalStateException("Length of blockLocations is " + blockLocations.length);
				}

				System.out.println("READ " + header.getPos());

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
