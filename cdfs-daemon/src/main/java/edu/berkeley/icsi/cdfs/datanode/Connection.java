package edu.berkeley.icsi.cdfs.datanode;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import edu.berkeley.icsi.cdfs.utils.ReliableDatagramSocket;

final class Connection extends Thread {

	private static final Log LOG = LogFactory.getLog(Connection.class);

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
		ReliableDatagramSocket socket = null;
		Closeable operation = null;

		// Read header first
		try {

			socket = new ReliableDatagramSocket();

			// Mode
			if (this.header.getConnectionMode() == ConnectionMode.WRITE) {

				// Send packet to inform client about new socket's port
				final byte[] buf = new byte[1];
				final DatagramPacket ackPacket = new DatagramPacket(buf, buf.length);
				ackPacket.setSocketAddress(this.remoteAddress);
				socket.send(ackPacket);

				// We are about to write to HDFS, prepare file system
				if (hdfs == null) {
					hdfs = CDFS.toHDFSPath(this.header.getPath(), "_0").getFileSystem(this.conf);
				}

				int blockIndex = 0;
				boolean readEOF = false;
				final PathWrapper cdfsPath = new PathWrapper(this.header.getPath());
				final WriteOp writeOp = new WriteOp(socket, hdfs, this.conf);
				operation = writeOp;
				while (!readEOF) {
					final Path hdfsPath = CDFS.toHDFSPath(this.header.getPath(), "_" + blockIndex);

					LOG.info("Writing block " + blockIndex + " to disk");
					readEOF = writeOp.write(hdfsPath, 128 * 1024 * 1024);

					// Report block information to name node
					synchronized (this.nameNode) {
						this.nameNode.createNewBlock(cdfsPath, new PathWrapper(hdfsPath), blockIndex,
							writeOp.getBytesWrittenInBlock());
					}

					// See if we had enough buffers to cache the written data
					final List<Buffer> uncompressedBuffers = writeOp.getUncompressedBuffers();
					if (!uncompressedBuffers.isEmpty()) {
						UncompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex, uncompressedBuffers);
						synchronized (this.nameNode) {
							this.nameNode.reportUncompressedCachedBlock(cdfsPath, blockIndex);
						}
					}

					final List<Buffer> compressedBuffers = writeOp.getCompressedBuffers();
					if (!compressedBuffers.isEmpty()) {
						CompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex, compressedBuffers);
						synchronized (this.nameNode) {
							this.nameNode.reportCompressedCachedBlock(cdfsPath, blockIndex);
						}
					}

					++blockIndex;
				}

			} else {

				final PathWrapper cdfsPath = new PathWrapper(this.header.getPath());

				CDFSBlockLocation[] blockLocations;
				synchronized (this.nameNode) {
					blockLocations = this.nameNode.getFileBlockLocations(new PathWrapper(this.header.getPath()),
						this.header.getPos(), 0L);
				}

				if (blockLocations == null) {
					throw new IllegalStateException("blockLocations is null");
				}

				if (blockLocations.length != 1) {
					throw new IllegalStateException("Length of blockLocations is " + blockLocations.length);
				}

				if (this.header.getPos() != blockLocations[0].getOffset()) {
					throw new IllegalStateException("Unable to seek to position other than block offset");
				}

				int blockIndex = blockLocations[0].getIndex();

				final ReadOp readOp = new ReadOp(socket, this.remoteAddress, this.conf);
				operation = readOp;

				while (true) {

					// See if we have the uncompressed version cached
					List<Buffer> uncompressedBuffers = UncompressedBufferCache.get().lock(this.header.getPath(),
						blockIndex);

					if (uncompressedBuffers != null) {
						try {
							LOG.info("Reading block " + blockIndex + " from cache (uncompressed), "
								+ uncompressedBuffers.size() + " buffers");
							readOp.readFromCacheUncompressed(uncompressedBuffers);
						} finally {
							UncompressedBufferCache.get().unlock(this.header.getPath(), blockIndex);
						}

						++blockIndex;
						continue;
					}

					// See if we have the compressed version cached
					List<Buffer> compressedBuffers = CompressedBufferCache.get().lock(header.getPath(),
						blockIndex);
					if (compressedBuffers != null) {
						try {
							LOG.info("Reading block " + blockIndex + " from cache (compressed), "
								+ compressedBuffers.size() + " buffers");
							readOp.readFromCacheCompressed(compressedBuffers);
						} finally {
							CompressedBufferCache.get().unlock(this.header.getPath(), blockIndex);
						}

						// See if we had enough buffers to cache the uncompressed data
						uncompressedBuffers = readOp.getUncompressedBuffers();
						if (!uncompressedBuffers.isEmpty()) {
							UncompressedBufferCache.get().addCachedBlock(this.header.getPath(), blockIndex,
								uncompressedBuffers);
							synchronized (this.nameNode) {
								this.nameNode.reportUncompressedCachedBlock(cdfsPath, blockIndex);
							}
						}

						++blockIndex;
						continue;
					}

					// We don't have the block cached, need to get it from HDFS
					final Path hdfsPath = CDFS.toHDFSPath(this.header.getPath(), "_" + blockIndex);
					if (hdfs == null) {
						// Create a file system object to ensure proper clean up
						hdfs = hdfsPath.getFileSystem(this.conf);
					}

					try {
						LOG.info("Reading block " + blockIndex + " from disk");
						readOp.readFromHDFSCompressed(hdfs, hdfsPath);
					} catch (FileNotFoundException fnfe) {
						break;
					}

					// See if we had enough buffers to cache the written data
					uncompressedBuffers = readOp.getUncompressedBuffers();
					if (!uncompressedBuffers.isEmpty()) {
						UncompressedBufferCache.get().addCachedBlock(this.header.getPath(), blockIndex,
							uncompressedBuffers);
						synchronized (this.nameNode) {
							this.nameNode.reportUncompressedCachedBlock(cdfsPath, blockIndex);
						}
					}

					compressedBuffers = readOp.getCompressedBuffers();
					if (!compressedBuffers.isEmpty()) {
						CompressedBufferCache.get()
							.addCachedBlock(this.header.getPath(), blockIndex, compressedBuffers);
						synchronized (this.nameNode) {
							this.nameNode.reportCompressedCachedBlock(cdfsPath, blockIndex);
						}
					}

					++blockIndex;
				}
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {

			try {
				// Close the operation
				if (operation != null) {
					operation.close();
				}

				// Close the socket
				if (socket != null) {
					socket.close();
				}

				// Close HDFS connection
				if (hdfs != null) {
					hdfs.close();
				}
			} catch (IOException ioe) {
			}
		}
	}
}
