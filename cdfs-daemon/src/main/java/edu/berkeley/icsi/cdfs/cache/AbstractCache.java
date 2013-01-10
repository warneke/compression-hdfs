package edu.berkeley.icsi.cdfs.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;

abstract class AbstractCache {

	private static final class BlockKey {

		private final String path;

		private final int index;

		private BlockKey(final Path path, final int index) {

			this.path = path.toUri().getPath();
			this.index = index;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(final Object obj) {

			if (!(obj instanceof BlockKey)) {
				return false;
			}

			final BlockKey bk = (BlockKey) obj;

			if (!this.path.equals(bk.path)) {
				return false;
			}

			if (this.index != bk.index) {
				return false;
			}

			return true;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {

			return this.path.hashCode() + this.index;
		}
	}

	private static final class CacheEntry {

		private final List<Buffer> cachedBuffers;

		private int lockCounter = 0;

		private CacheEntry(final List<Buffer> cachedBuffers) {
			this.cachedBuffers = cachedBuffers;
		}
	}

	private final Map<BlockKey, CacheEntry> cache = new HashMap<BlockKey, CacheEntry>();

	public List<Buffer> lock(final Path path, final int blockIndex) {

		final BlockKey bk = new BlockKey(path, blockIndex);

		synchronized (this) {

			final CacheEntry entry = this.cache.get(bk);
			if (entry == null) {
				return null;
			}

			entry.lockCounter++;

			return entry.cachedBuffers;
		}
	}

	public void unlock(final Path path, final int blockIndex) {

		final BlockKey bk = new BlockKey(path, blockIndex);

		synchronized (this) {

			final CacheEntry entry = this.cache.get(bk);
			if (entry == null) {
				throw new IllegalStateException("Cannot find entry for path " + path);
			}

			entry.lockCounter--;

			if (entry.lockCounter < 0) {
				throw new IllegalStateException("Lock counter for path " + path + " is " + entry.lockCounter);
			}
		}
	}

	public void addCachedBlock(final Path path, final int blockIndex, final List<Buffer> buffers) {

		final BlockKey bk = new BlockKey(path, blockIndex);

		synchronized (this) {

			if (this.cache.containsKey(bk)) {
				// Another has already been added for the same block in the meantime
				throw new IllegalStateException("destroyCachedBlock(cachedBlock)");
			}

			getLog().info("Adding " + path + " to cache (" + buffers.size() + " buffers)");
			this.cache.put(bk, new CacheEntry(buffers));
		}
	}

	protected abstract Log getLog();
}
