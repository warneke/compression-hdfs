package edu.berkeley.icsi.cdfs.cache;

import org.apache.hadoop.fs.Path;

public class BlockKey {

	private final String path;

	private final int index;

	public BlockKey(final String path, final int index) {
		this.path = path;
		this.index = index;
	}

	public BlockKey(final Path path, final int index) {
		this(path.toUri().getPath(), index);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder(this.path);
		sb.append(' ');
		sb.append('(');
		sb.append(this.index);
		sb.append(')');

		return sb.toString();
	}
}
