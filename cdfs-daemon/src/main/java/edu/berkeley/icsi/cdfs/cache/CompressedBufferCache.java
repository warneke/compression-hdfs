package edu.berkeley.icsi.cdfs.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class CompressedBufferCache extends AbstractCache {

	private static final Log LOG = LogFactory.getLog(CompressedBufferCache.class);

	private static final CompressedBufferCache INSTANCE = new CompressedBufferCache();

	private CompressedBufferCache() {
	}

	public static CompressedBufferCache get() {

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
