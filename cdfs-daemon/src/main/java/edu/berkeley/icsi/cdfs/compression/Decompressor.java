package edu.berkeley.icsi.cdfs.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

public final class Decompressor {

	public int decompress(final byte[] input, final int inputOffset, final int inputLength, final byte[] output)
			throws IOException {

		return Snappy.uncompress(input, inputOffset, inputLength, output, 0);
	}

}
