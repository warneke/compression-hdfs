package edu.berkeley.icsi.cdfs.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

public final class Compressor {

	private final byte[] compressedBuffer;

	public Compressor(final int bufferSize) {

		this.compressedBuffer = new byte[Snappy.maxCompressedLength(bufferSize)];
	}

	public int compress(final byte[] input, final int inputLength) throws IOException {

		return Snappy.compress(input, 0, inputLength, this.compressedBuffer, 0);
	}

	public byte[] getCompressedBuffer() {

		return this.compressedBuffer;
	}
}
