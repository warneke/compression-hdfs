package edu.berkeley.icsi.cdfs.datanode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.CompressedBufferCache;
import edu.berkeley.icsi.cdfs.cache.UncompressedBufferCache;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

final class Connection extends Thread {

	private final SocketAddress remoteAddress;

	private final Header header;

	private final DataNodeNameNodeProtocol nameNode;

	private final Configuration conf;

	Connection(final SocketAddress remoteAddress, final Header header, final DataNodeNameNodeProtocol nameNode,
			final Configuration conf) {
		super("DataNodeConnection from " + remoteAddress);

		this.remoteAddress = remoteAddress;
		this.header = header;
		this.nameNode = nameNode;
		this.conf = conf;
		start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// Reference to HDFS
		FileSystem hdfs = null;

		// Socket for control messages
		DatagramSocket socket = null;

		// Read header first
		try {

			socket = new DatagramSocket();

			// Mode
			if (header.getConnectionMode() == ConnectionMode.WRITE) {

				// Send packet to inform client about new socket's port
				final byte[] buf = new byte[1];
				final DatagramPacket ackPacket = new DatagramPacket(buf, buf.length);
				ackPacket.setSocketAddress(this.remoteAddress);
				socket.send(ackPacket);

				// We are about to write to HDFS, prepare file system
				if (hdfs == null) {
					hdfs = CDFS.toHDFSPath(header.getPath(), "_0").getFileSystem(this.conf);
				}

				int blockIndex = 0;
				boolean readEOF = false;
				final PathWrapper cdfsPath = new PathWrapper(header.getPath());
				while (!readEOF) {
					final Path hdfsPath = CDFS.toHDFSPath(header.getPath(), "_" + blockIndex);
					final WriteOp wo = new WriteOp(hdfs, hdfsPath, 128 * 1024 * 1024, this.conf);
					// TODO: Fixe me
					// readEOF = wo.write(inputStream);

					// Report block information to name node
					synchronized (this.nameNode) {
						this.nameNode.createNewBlock(cdfsPath, new PathWrapper(hdfsPath), blockIndex,
							wo.getBytesWrittenInBlock());
					}

					// See if we had enough buffers to cache the written data
					final List<Buffer> uncompressedBuffers = wo.getUncompressedBuffers();
					if (!uncompressedBuffers.isEmpty()) {
						UncompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex, uncompressedBuffers);
						synchronized (this.nameNode) {
							this.nameNode.reportUncompressedCachedBlock(cdfsPath, blockIndex);
						}
					}

					final List<Buffer> compressedBuffers = wo.getCompressedBuffers();
					if (!compressedBuffers.isEmpty()) {
						CompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex, compressedBuffers);
						synchronized (this.nameNode) {
							this.nameNode.reportCompressedCachedBlock(cdfsPath, blockIndex);
						}
					}

					++blockIndex;
				}

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

				if (header.getPos() != blockLocations[0].getOffset()) {
					throw new IllegalStateException("Unable to seek to position other than block offset");
				}

				int blockIndex = blockLocations[0].getIndex();

				while (true) {

					// See if we have the uncompressed version cached
					final List<Buffer> uncompressedBuffers = UncompressedBufferCache.get().lock(header.getPath(),
						blockIndex);
					if (uncompressedBuffers != null) {
						try {
							System.out.println("Reading block " + blockIndex + " from cache (uncompressed)");
							final UncompressedCachedReadOp ro = new UncompressedCachedReadOp(uncompressedBuffers);
							ro.read(this.remoteAddress);
						} finally {
							UncompressedBufferCache.get().unlock(header.getPath(), blockIndex);
						}
					} else {

						// See if we have the compressed version cached
						final List<Buffer> compressedBuffers = CompressedBufferCache.get().lock(header.getPath(),
							blockIndex);
						if (compressedBuffers != null) {
							try {
								System.out.println("Reading block " + blockIndex + " from cache (compressed)");
								final CompressedCachedReadOp ro = new CompressedCachedReadOp(compressedBuffers);
								ro.read(this.remoteAddress);
							} finally {
								CompressedBufferCache.get().unlock(header.getPath(), blockIndex);
							}
						} else {

							// We are about to read from HDFS, prepare file system
							if (hdfs == null) {
								hdfs = CDFS.toHDFSPath(header.getPath(), "_0").getFileSystem(this.conf);
							}

							final CachingReadOp ro = new CachingReadOp(hdfs, CDFS.toHDFSPath(header.getPath(), "_"
								+ blockIndex), this.conf);

							try {
								ro.read(this.remoteAddress);
							} catch (FileNotFoundException e) {
								break;
							}
						}
					}

					++blockIndex;
				}
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {

			// Close the socket
			if (socket != null) {
				socket.close();
			}

			// Close HDFS connection
			if (hdfs != null) {
				try {
					hdfs.close();
				} catch (IOException ioe) {
				}
			}
		}
	}
}
