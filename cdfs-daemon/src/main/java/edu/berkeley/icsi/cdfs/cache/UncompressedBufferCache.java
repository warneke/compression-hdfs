package edu.berkeley.icsi.cdfs.cache;

public final class UncompressedBufferCache extends AbstractCache {

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
	protected String getName() {

		return "UncompressedCache";
	}
}
