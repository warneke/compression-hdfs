package edu.berkeley.icsi.cdfs.datanode;

import java.io.Closeable;
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
		Closeable operation = null;

		// Read header first
		try {

			socket = new DatagramSocket();

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
				final WriteOp wo = new WriteOp(socket, hdfs, this.conf);
				operation = wo;
				while (!readEOF) {
					final Path hdfsPath = CDFS.toHDFSPath(this.header.getPath(), "_" + blockIndex);

					readEOF = wo.write(hdfsPath, 128 * 1024 * 1024);

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
					final List<Buffer> uncompressedBuffers = UncompressedBufferCache.get().lock(header.getPath(),
						blockIndex);

					if (uncompressedBuffers != null) {
						try {
							System.out.println("Reading block " + blockIndex + " from cache (uncompressed) "
								+ uncompressedBuffers.size());
							readOp.readFromCacheUncompressed(uncompressedBuffers);
						} finally {
							UncompressedBufferCache.get().unlock(this.header.getPath(), blockIndex);
						}

						++blockIndex;
						continue;
					}

					// See if we have the compressed version cached
					final List<Buffer> compressedBuffers = CompressedBufferCache.get().lock(header.getPath(),
						blockIndex);
					if (compressedBuffers != null) {
						try {
							System.out.println("Reading block " + blockIndex + " from cache (compressed) "
								+ compressedBuffers.size());
							readOp.readFromCacheCompressed(compressedBuffers);
						} finally {
							CompressedBufferCache.get().unlock(this.header.getPath(), blockIndex);
						}

						// TODO: Check if we cached the uncompressed version

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
						readOp.readFromHDFSCompressed(hdfs, hdfsPath);
					} catch (FileNotFoundException fnfe) {
						break;
					}

					// TODO: Check if we cached the uncompressed or compressed version

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
