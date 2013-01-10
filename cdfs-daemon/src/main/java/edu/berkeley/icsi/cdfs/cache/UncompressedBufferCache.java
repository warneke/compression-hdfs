package edu.berkeley.icsi.cdfs.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class UncompressedBufferCache extends AbstractCache {

	private static final Log LOG = LogFactory.getLog(UncompressedBufferCache.class);

	private static final UncompressedBufferCache INSTANCE = new UncompressedBufferCache();

	private UncompressedBufferCache() {
	}

	public static UncompressedBufferCache get() {

		return INSTANCE;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Log getLog() {

		return LOG;
	}
}
