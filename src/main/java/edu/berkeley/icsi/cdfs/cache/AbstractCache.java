package edu.berkeley.icsi.cdfs.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;

abstract class AbstractCache {

	private static final class CacheEntry {

		private final List<Buffer> cachedBuffers;

		private int lockCounter = 0;

		private CacheEntry(final List<Buffer> cachedBuffers) {
			this.cachedBuffers = cachedBuffers;
		}
	}

	private final Map<Path, CacheEntry> cache = new HashMap<Path, CacheEntry>();

	public List<Buffer> lock(final Path path) {

		synchronized (this) {

			final CacheEntry entry = this.cache.get(path);
			if (entry == null) {
				return null;
			}

			entry.lockCounter++;

			return entry.cachedBuffers;
		}
	}

	public void unlock(final Path path) {

		synchronized (this) {

			final CacheEntry entry = this.cache.get(path);
			if (entry == null) {
				throw new IllegalStateException("Cannot find entry for path " + path);
			}

			entry.lockCounter--;

			if (entry.lockCounter < 0) {
				throw new IllegalStateException("Lock counter for path " + path + " is " + entry.lockCounter);
			}
		}
	}

	public void addCachedBlock(final Path path, final List<Buffer> buffers) {

		synchronized (this) {

			if (this.cache.containsKey(path)) {
				// Another has already been added for the same block in the meantime
				throw new IllegalStateException("destroyCachedBlock(cachedBlock)");
			}

			getLog().info("Adding " + path + " to cache (" + buffers.size() + " buffers)");
			this.cache.put(path, new CacheEntry(buffers));
		}
	}

	protected abstract Log getLog();
}
