package edu.berkeley.icsi.cdfs.utils;

import edu.berkeley.icsi.cdfs.PopularBlock;

public final class CompressionUtils {

	private static final double MINIMUM_COMPRESSION_RATIO = 1.6;

	private CompressionUtils() {
	}

	public static boolean isCompressible(final PopularBlock popularBlock) {

		return isCompressible(popularBlock.getUncompressedLength(), popularBlock.getCompressedLength());
	}

	private static boolean isCompressible(final long uncompressedSize, final long compressedSize) {

		final double ratio = (double) uncompressedSize / (double) compressedSize;

		return (ratio > MINIMUM_COMPRESSION_RATIO);
	}
}
