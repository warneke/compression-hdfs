package edu.berkeley.icsi.cdfs.datanode;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.BlockReadInformation;
import edu.berkeley.icsi.cdfs.ConnectionMode;
import edu.berkeley.icsi.cdfs.Header;
import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.CompressedBufferCache;
import edu.berkeley.icsi.cdfs.cache.UncompressedBufferCache;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.statistics.ReadStatistics;
import edu.berkeley.icsi.cdfs.utils.PathConverter;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;
import edu.berkeley.icsi.cdfs.utils.WritableArrayList;

final class Connection extends Thread {

	private static final Log LOG = LogFactory.getLog(Connection.class);

	private final Socket socket;

	private final DataNodeNameNodeProtocol nameNode;

	private final Configuration conf;

	private final String host;

	private final FileSystem hdfs;

	private final PathConverter pathConverter;

	private final ConnectionDispatcher connectionDispatcher;

	Connection(final Socket socket, final DataNodeNameNodeProtocol nameNode,
			final Configuration conf, final String host, final FileSystem hdfs, final PathConverter pathConverter,
			final ConnectionDispatcher connectionDispatcher) {
		super("DataNodeConnection from " + socket.getRemoteSocketAddress());

		this.socket = socket;
		this.nameNode = nameNode;
		this.conf = conf;
		this.host = host;
		this.hdfs = hdfs;
		this.pathConverter = pathConverter;
		this.connectionDispatcher = connectionDispatcher;
		start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// The operation this connection handles
		Closeable operation = null;

		// List to hold the statistics
		final WritableArrayList<ReadStatistics> readStatistics = new WritableArrayList<ReadStatistics>();

		// Read header first
		try {

			final Header header = Header.fromInputStream(this.socket.getInputStream());

			LOG.info("Starting connection for " + header);

			// Mode
			if (header.getConnectionMode() == ConnectionMode.WRITE) {

				int blockIndex = 0;
				boolean readEOF = false;
				final PathWrapper cdfsPath = new PathWrapper(header.getPath());
				final WriteOp writeOp = new WriteOp(this.socket, this.hdfs, this.conf);
				operation = writeOp;
				while (!readEOF) {
					final Path hdfsPath = this.pathConverter.convert(header.getPath(), "_" + blockIndex);

					LOG.info("Writing block " + blockIndex + " to disk");
					readEOF = writeOp.write(hdfsPath, ConfigConstants.BLOCK_SIZE);

					// Report block information to name node
					synchronized (this.nameNode) {
						this.nameNode.createNewBlock(cdfsPath, new PathWrapper(hdfsPath), blockIndex,
							writeOp.getUncompressedBytesWrittenInBlock(), writeOp.getCompressedBytesWrittenInBlock());
					}

					// See if we had enough buffers to cache the written data
					final List<Buffer> uncompressedBuffers = writeOp.getUncompressedBuffers();
					if (!uncompressedBuffers.isEmpty()) {
						if (UncompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex,
							uncompressedBuffers)) {
							synchronized (this.nameNode) {
								this.nameNode.reportCachedBlock(cdfsPath, blockIndex, false, this.host);
							}
						}
					}

					final List<Buffer> compressedBuffers = writeOp.getCompressedBuffers();
					if (!compressedBuffers.isEmpty()) {
						if (CompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex, compressedBuffers)) {
							synchronized (this.nameNode) {
								this.nameNode.reportCachedBlock(cdfsPath, blockIndex, true, this.host);
							}
						}
					}

					++blockIndex;
				}

			} else {

				final PathWrapper cdfsPath = new PathWrapper(header.getPath());

				BlockReadInformation[] blockReadInformation;
				synchronized (this.nameNode) {
					blockReadInformation = this.nameNode.getBlockReadInformation(new PathWrapper(header.getPath()),
						header.getPos(), 0L);
				}

				if (blockReadInformation == null) {
					throw new IllegalStateException("blockLocations is null");
				}

				if (blockReadInformation.length != 1) {
					throw new IllegalStateException("Length of blockLocations is " + blockReadInformation.length);
				}

				if (header.getPos() != blockReadInformation[0].getOffset()) {
					throw new IllegalStateException("Unable to seek to position other than block offset");
				}

				final int totalNumberOfBlocks = blockReadInformation[0].getTotalNumberOfBlocks();
				int blockIndex = blockReadInformation[0].getIndex();

				final ReadOp readOp = new ReadOp(this.socket);
				operation = readOp;
				boolean runLoop = true;

				final boolean cacheUncompressed = blockReadInformation[0].cacheUncompressed();
				final boolean cacheCompressed = blockReadInformation[0].cacheCompressed();

				while ((blockIndex < totalNumberOfBlocks) && runLoop) {

					// Determine the expected length of the block
					final long blockLength = (blockIndex == blockReadInformation[0].getIndex()) ? blockReadInformation[0]
						.getLength()
						: -1L;
					LOG.info("Determined length of block " + blockIndex + " to be " + blockLength + " bytes");

					// See if we have the uncompressed version cached
					List<Buffer> uncompressedBuffers = UncompressedBufferCache.get().lock(header.getPath(), blockIndex);

					if (uncompressedBuffers != null) {
						try {
							readStatistics.add(ReadStatistics.createCacheUncompressed(header.getPath(), blockIndex));
							LOG.info("Reading block " + blockIndex + " of " + header.getPath()
								+ " from cache (uncompressed), " + uncompressedBuffers.size() + " buffers");
							readOp.readFromCacheUncompressed(uncompressedBuffers);
						} catch (EOFException e) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Caught EOFException from readFromCacheUncompressed after " +
									+readOp.getNumberOfBytesRead() + " bytes");
							}
							runLoop = false;
						} finally {
							UncompressedBufferCache.get().unlock(header.getPath(), blockIndex);
						}

						++blockIndex;
						continue;
					}

					// See if we have the compressed version cached
					List<Buffer> compressedBuffers = CompressedBufferCache.get().lock(header.getPath(),
						blockIndex);
					if (compressedBuffers != null) {
						try {
							readStatistics.add(ReadStatistics.createCacheCompressed(header.getPath(), blockIndex));
							LOG.info("Reading block " + blockIndex + " of " + header.getPath()
								+ " from cache (compressed), " + compressedBuffers.size() + " buffers");
							readOp.readFromCacheCompressed(compressedBuffers, cacheUncompressed);
						} catch (EOFException e) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Caught EOFException from readFromCacheCompressed after "
									+ readOp.getNumberOfBytesRead() + " bytes");
							}
							runLoop = false;
						} finally {
							CompressedBufferCache.get().unlock(header.getPath(), blockIndex);
						}

						if (!readOp.isBlockFullyRead(blockLength)) {
							if (runLoop) {
								throw new IllegalStateException("No EOFException but incomplete block read");
							}

							LOG.info("Aborted read of block " + blockIndex + " after " + readOp.getNumberOfBytesRead()
								+ " bytes");
							break;
						}

						// See if we had enough buffers to cache the uncompressed data
						uncompressedBuffers = readOp.getUncompressedBuffers();
						if (!uncompressedBuffers.isEmpty()) {
							if (UncompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex,
								uncompressedBuffers)) {
								synchronized (this.nameNode) {
									this.nameNode.reportCachedBlock(cdfsPath, blockIndex, false, this.host);
								}
							}
						}

						++blockIndex;
						continue;
					}

					// We don't have the block cached, need to get it from HDFS
					final Path hdfsPath = this.pathConverter.convert(header.getPath(), "_" + blockIndex);

					try {
						readStatistics.add(ReadStatistics.createDisk(header.getPath(), blockIndex));
						LOG.info("Reading block " + blockIndex + " of " + header.getPath() + " from disk");
						readOp.readFromHDFSCompressed(this.hdfs, hdfsPath, cacheUncompressed, cacheCompressed);
					} catch (EOFException e) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Caught EOFException from readFromHDFSCompressed after "
								+ readOp.getNumberOfBytesRead() + " bytes");
						}
						runLoop = false;
					} catch (FileNotFoundException fnfe) {
						LOG.error(StringUtils.stringifyException(fnfe));
						break;
					}

					if (!readOp.isBlockFullyRead(blockLength)) {
						if (runLoop) {
							throw new IllegalStateException("No EOFException but incomplete block read");
						}

						LOG.info("Aborted read of block " + blockIndex + " after " + readOp.getNumberOfBytesRead()
							+ " bytes");
						break;
					}

					// See if we had enough buffers to cache the written data
					uncompressedBuffers = readOp.getUncompressedBuffers();
					if (!uncompressedBuffers.isEmpty()) {
						if (UncompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex,
							uncompressedBuffers)) {
							synchronized (this.nameNode) {
								this.nameNode.reportCachedBlock(cdfsPath, blockIndex, false, this.host);
							}
						}
					}

					compressedBuffers = readOp.getCompressedBuffers();
					if (!compressedBuffers.isEmpty()) {
						if (CompressedBufferCache.get().addCachedBlock(header.getPath(), blockIndex, compressedBuffers)) {
							synchronized (this.nameNode) {
								this.nameNode.reportCachedBlock(cdfsPath, blockIndex, true, this.host);
							}
						}
					}

					++blockIndex;
				}
			}

			LOG.info("Finishing connection for " + header);

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {

			try {
				// Close the operation
				if (operation != null) {
					operation.close();
				}

				// Close the socket
				if (this.socket != null) {
					this.socket.close();
				}

				// Transmit statistics
				synchronized (this.nameNode) {
					this.nameNode.reportReadStatistics(readStatistics, this.host);
				}

			} catch (IOException ioe) {
			}

			// Remove connection object
			this.connectionDispatcher.removeConnection(this);
		}
	}
}
