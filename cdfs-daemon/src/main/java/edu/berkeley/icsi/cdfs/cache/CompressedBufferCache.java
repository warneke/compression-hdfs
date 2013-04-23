package edu.berkeley.icsi.cdfs.cache;

public final class CompressedBufferCache extends AbstractCache {

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
	protected String getName() {

		return "CompressedCache";
	}
}
