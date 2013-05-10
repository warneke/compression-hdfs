package edu.berkeley.icsi.cdfs.cache;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;

public final class BufferPool {

	private static final Log LOG = LogFactory.getLog(BufferPool.class);

	/**
	 * The names of the tenured memory pool
	 */
	private static final String[] TENURED_POOL_NAMES = { "Tenured Gen", "PS Old Gen", "CMS Old Gen" };

	/**
	 * The memory threshold to be used when tenured pool can be determined
	 */
	private static float TENURED_POOL_THRESHOLD = 0.95f;

	/**
	 * The singleton instance of the buffer pool
	 */
	private static BufferPool INSTANCE = null;

	private final DataNodeNameNodeProtocol nameNode;

	private final String host;

	private final ArrayBlockingQueue<byte[]> buffers;

	private BufferPool(final DataNodeNameNodeProtocol nameNode, final String host) {

		this.nameNode = nameNode;
		this.host = host;

		final long availableMemoryForBuffers = getSizeOfFreeMemory();
		final int numberOfBuffers = (int) (availableMemoryForBuffers / ConfigConstants.BUFFER_SIZE);

		LOG.info("Initialized buffer pool with " + availableMemoryForBuffers + " bytes of memory, creating "
			+ numberOfBuffers + " buffers");

		this.buffers = new ArrayBlockingQueue<byte[]>(numberOfBuffers);

		for (int i = 0; i < numberOfBuffers; ++i) {
			this.buffers.add(new byte[ConfigConstants.BUFFER_SIZE]);
		}
	}

	/**
	 * Returns the size of free memory in bytes available to the JVM.
	 * 
	 * @return the size of the free memory in bytes available to the JVM or <code>-1</code> if the size cannot be
	 *         determined
	 */
	private static long getSizeOfFreeMemory() {

		// in order to prevent allocations of arrays that are too big for the
		// JVM's different memory pools,
		// make sure that the maximum segment size is 70% of the currently free
		// tenure heap
		final MemoryPoolMXBean tenuredpool = findTenuredGenPool();

		if (tenuredpool != null) {
			final MemoryUsage usage = tenuredpool.getUsage();
			long tenuredSize = usage.getMax() - usage.getUsed();
			LOG.info("Found Tenured Gen pool (max: " + tenuredSize + ", used: " + usage.getUsed() + ")");
			// TODO: make the constant configurable
			return (long) (tenuredSize * TENURED_POOL_THRESHOLD);
		}

		throw new IllegalStateException("Could not find tenured gen pool");
	}

	/**
	 * Returns the tenured gen pool.
	 * 
	 * @return the tenured gen pool or <code>null</code> if so such pool can be
	 *         found
	 */
	private static MemoryPoolMXBean findTenuredGenPool() {
		for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {

			for (String s : TENURED_POOL_NAMES) {
				if (pool.getName().equals(s)) {
					// seems that we found the tenured pool
					// double check, if it MemoryType is HEAP and usageThreshold
					// supported..
					if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
						return pool;
					}
				}
			}
		}
		return null;
	}

	public static synchronized BufferPool get() {

		if (INSTANCE == null) {
			throw new IllegalStateException("Buffer pool has not been initialized");
		}

		return INSTANCE;
	}

	public static synchronized void initialize(final DataNodeNameNodeProtocol nameNode, final String host) {

		if (INSTANCE != null) {
			throw new IllegalStateException("Buffer pool has already been initialized");
		}

		INSTANCE = new BufferPool(nameNode, host);
	}

	public byte[] lockBuffer() throws IOException {

		byte[] buffer = this.buffers.poll();

		while (buffer == null) {

			// Do cache eviction
			final EvictionList el;
			synchronized (this.nameNode) {
				el = this.nameNode.getFilesToEvict(this.host);
			}

			final Iterator<ExtendedBlockKey> it = el.iterator();
			while (it.hasNext()) {

				final ExtendedBlockKey blockKey = it.next();
				if (blockKey.getCompressed()) {
					CompressedBufferCache.get().evict(blockKey);
				} else {
					UncompressedBufferCache.get().evict(blockKey);
				}
			}

			buffer = this.buffers.poll();
		}

		return buffer;
	}

	public void releaseBuffer(final byte[] buffer) {

		this.buffers.add(buffer);
	}

	public int getNumberOfAvailableBuffers() {

		return this.buffers.size();
	}
}
