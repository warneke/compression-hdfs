package edu.berkeley.icsi.cdfs.cache;

public final class Buffer {

	private final byte[] data;

	private final int length;

	public Buffer(final byte[] data, final int length) {
		this.data = data;
		this.length = length;
	}

	public byte[] getData() {
		return this.data;
	}

	public int getLength() {
		return this.length;
	}
}