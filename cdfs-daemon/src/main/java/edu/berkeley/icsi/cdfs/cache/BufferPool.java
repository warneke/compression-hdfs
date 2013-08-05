package edu.berkeley.icsi.cdfs.cache;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

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

	private final boolean autoEvict;

	private BufferPool(final DataNodeNameNodeProtocol nameNode, final String host, final boolean autoEvict) {

		this.nameNode = nameNode;
		this.host = host;
		this.autoEvict = autoEvict;

		final long availableMemoryForBuffers = getSizeOfFreeMemory();
		final int numberOfBuffers = (int) (availableMemoryForBuffers / ConfigConstants.BUFFER_SIZE);

		LOG.info("Initialized buffer pool with " + availableMemoryForBuffers + " bytes of memory, creating "
			+ numberOfBuffers + " buffers (auto evict " + (autoEvict ? "enabled" : "disabled") + ")");

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

	public static synchronized void initialize(final DataNodeNameNodeProtocol nameNode, final String host,
			final boolean autoEvict) {

		if (INSTANCE != null) {
			throw new IllegalStateException("Buffer pool has already been initialized");
		}

		INSTANCE = new BufferPool(nameNode, host, autoEvict);
	}

	public byte[] lockBuffer() throws IOException {

		byte[] buffer = this.buffers.poll();

		while (buffer == null && this.autoEvict) {

			// Do cache eviction
			final EvictionEntry ee;
			synchronized (this.nameNode) {
				ee = this.nameNode.getFileToEvict(this.host);
			}

			if (ee == null) {
				continue;
			}

			final PathWrapper pw = ee.getPathWrapper();
			final Path path = pw.getPath();

			if (LOG.isInfoEnabled()) {
				LOG.info("Evicting " + path + " (" + (ee.isCompressed() ? "compressed" : "uncompressed") + ")");
			}

			final AbstractCache cache;
			if (ee.isCompressed()) {
				cache = CompressedBufferCache.get();
			} else {
				cache = UncompressedBufferCache.get();
			}

			final int numberOfBlocks = ee.getNumberOfBlocks();
			for (int i = 0; i < numberOfBlocks; ++i) {
				final List<Buffer> evictedBuffers = cache.evict(path, i);
				if (evictedBuffers != null) {
					final Iterator<Buffer> it = evictedBuffers.iterator();
					while (it.hasNext()) {
						this.buffers.add(it.next().getData());
					}
					synchronized (this.nameNode) {
						this.nameNode.confirmEviction(pw, i, ee.isCompressed(), this.host);
					}
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

	public long getAvaiableBufferSpace() {

		return (long) this.buffers.size() * (long) ConfigConstants.BUFFER_SIZE;
	}

	public static long sizeInCache(final long size) {

		return ((size + ConfigConstants.BUFFER_SIZE - 1) / ConfigConstants.BUFFER_SIZE)
			* (long) ConfigConstants.BUFFER_SIZE;
	}
}
