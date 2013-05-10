package edu.berkeley.icsi.cdfs.cache;

public final class ExtendedBlockKey extends BlockKey {

	private final boolean compressed;

	public ExtendedBlockKey(final String path, final int index, final boolean compressed) {
		super(path, index);

		this.compressed = compressed;
	}

	public boolean getCompressed() {

		return this.compressed;
	}
}
