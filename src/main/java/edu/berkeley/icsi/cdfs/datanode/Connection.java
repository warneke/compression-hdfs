package edu.berkeley.icsi.cdfs.datanode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
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

	private final Socket socket;

	private final DataNodeNameNodeProtocol nameNode;

	private final Configuration conf;

	Connection(final Socket socket, final DataNodeNameNodeProtocol nameNode, final Configuration conf) {
		super("DataNodeConnection from " + socket.getRemoteSocketAddress());

		this.socket = socket;
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

		// Read header first
		try {
			final InputStream inputStream = this.socket.getInputStream();

			final Header header = Header.receiveHeader(inputStream);
			if (header == null) {
				throw new IOException("Unexpected end of header");
			}

			// Mode
			if (header.getConnectionMode() == ConnectionMode.WRITE) {

				// We are about to write to HDFS, prepare file system
				if (hdfs == null) {
					hdfs = CDFS.toHDFSPath(header.getPath(), "_0").getFileSystem(this.conf);
				}

				int blockIndex = 0;
				boolean readEOF = false;
				final PathWrapper cdfsPath = new PathWrapper(header.getPath());
				while (!readEOF) {
					final Path hdfsPath = CDFS.toHDFSPath(header.getPath(), "_" + blockIndex);
					final WriteOp wo = new WriteOp(hdfs, hdfsPath, 128 * 1024 * 1024);
					readEOF = wo.write(inputStream);

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
							ro.read(this.socket.getOutputStream());
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
								ro.read(this.socket.getOutputStream());
							} finally {
								CompressedBufferCache.get().unlock(header.getPath(), blockIndex);
							}
						} else {

							// We are about to read from HDFS, prepare file system
							if (hdfs == null) {
								hdfs = CDFS.toHDFSPath(header.getPath(), "_0").getFileSystem(this.conf);
							}

							final CachingReadOp ro = new CachingReadOp(hdfs, CDFS.toHDFSPath(header.getPath(), "_"
								+ blockIndex));

							try {
								ro.read(this.socket.getOutputStream());
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
			if (this.socket != null) {
				try {
					this.socket.close();
				} catch (IOException ioe) {
				}
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
