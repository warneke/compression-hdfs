package edu.berkeley.icsi.cdfs.tracegen;

final class File {

	private final long uncompressedFileSize;

	private final int compressionFactor;

	File(final long uncompressedFileSize, final int compressionFactor) {

		this.uncompressedFileSize = uncompressedFileSize;
		this.compressionFactor = compressionFactor;
	}

	long getUncompressedFileSize() {
		return this.uncompressedFileSize;
	}

	long getCompressedFileSize() {
		return this.uncompressedFileSize / this.compressionFactor;
	}

	int getCompressionFactor() {
		return this.compressionFactor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return "file_" + this.uncompressedFileSize + "_" + compressionFactor;
	}
}
