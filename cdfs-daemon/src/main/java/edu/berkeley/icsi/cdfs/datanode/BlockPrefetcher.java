package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.PopularBlock;
import edu.berkeley.icsi.cdfs.PopularFile;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.cache.CompressedBufferCache;
import edu.berkeley.icsi.cdfs.cache.UncompressedBufferCache;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;

final class BlockPrefetcher extends Thread {

	private static final Log LOG = LogFactory.getLog(BlockPrefetcher.class);

	private static final int SLEEP_INTERVAL = 5000;

	private static final int MAXIMUM_NUMBER_OF_FILES = 10;

	private final ConnectionDispatcher connectionDispatcher;

	private final DataNodeNameNodeProtocol nameNode;

	private volatile boolean shutDownRequested = false;

	BlockPrefetcher(final ConnectionDispatcher connectionDispatcher, final DataNodeNameNodeProtocol nameNode) {
		super("Prefetcher thread");

		this.connectionDispatcher = connectionDispatcher;
		this.nameNode = nameNode;

		start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		final UncompressedBufferCache uncompressedBufferCache = UncompressedBufferCache.get();
		final CompressedBufferCache compressedBufferCache = CompressedBufferCache.get();
		final BufferPool bufferPool = BufferPool.get();

		final Random rand = new Random();

		while (!this.shutDownRequested) {

			try {
				sleep(SLEEP_INTERVAL);
			} catch (InterruptedException e) {
				continue;
			}

			// Check if data node is idle
			if (this.connectionDispatcher.hasActiveConnections()) {
				continue;
			}

			// Retrieve list of most popular files
			final PopularFile popularFiles[];
			try {
				synchronized (this.nameNode) {
					popularFiles = this.nameNode.getPopularFiles(MAXIMUM_NUMBER_OF_FILES);
				}
			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
				continue;
			}

			for (int i = 0; i < popularFiles.length; ++i) {

				final PopularFile popularFile = popularFiles[i];

				// Are we interested in this file?
				if (rand.nextDouble() > popularFile.getPopularityFactor()) {
					continue;
				}

				final Path path = popularFile.getPath();
				final int numberOfBlocks = popularFile.getNumberOfBlocks();

				for (int j = 0; j < numberOfBlocks; ++j) {

					final PopularBlock popularBlock = popularFile.getBlock(j);
					final int index = popularBlock.getIndex();

					// Is block already cached?
					if (uncompressedBufferCache.contains(path, index) || compressedBufferCache.contains(path, index)) {
						continue;
					}

					// Does the block fit into the cache
					if (BufferPool.sizeInCache(popularBlock.getCompressedSize()) > bufferPool.getAvaiableBufferSpace()) {
						continue;
					}

					// Make sure we do not cache all the blocks
					if (rand.nextInt(numberOfBlocks) != index) {
						continue;
					}

					LOG.info("Prefetching block " + path + " " + index);
				}
			}
		}
	}

	void shutDown() {

		this.shutDownRequested = true;
		interrupt();

		try {
			join();
		} catch (InterruptedException e) {
			LOG.warn(StringUtils.stringifyException(e));
		}
	}
}
