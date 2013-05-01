package edu.berkeley.icsi.cdfs.wlgen;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class File {

	private final Set<MapReduceJob> usedAsInput = new HashSet<MapReduceJob>();

	private final Set<MapReduceJob> usedAsOutput = new HashSet<MapReduceJob>();

	private final String name;

	private final long uncompressedFileSize;

	private final int compressionRatio;

	File(final long uncompressedFileSize, final int compressionRatio) {

		this.name = "file_" + uncompressedFileSize;
		this.uncompressedFileSize = uncompressedFileSize;
		this.compressionRatio = compressionRatio;
	}

	public String getName() {

		return this.name;
	}

	public long getUncompressedFileSize() {

		return this.uncompressedFileSize;
	}

	public int getCompressionRatio() {

		return this.compressionRatio;
	}

	void usedAsInputBy(final MapReduceJob mapReduceJob) {

		this.usedAsInput.add(mapReduceJob);
	}

	void usedAsOutputBy(final MapReduceJob mapReduceJob) {

		this.usedAsOutput.add(mapReduceJob);
	}

	Iterator<MapReduceJob> inputIterator() {

		return this.usedAsInput.iterator();
	}

	Iterator<MapReduceJob> outputIterator() {

		return this.usedAsOutput.iterator();
	}

	int getNumberOfInputUsages() {

		return this.usedAsInput.size();
	}

	int getNumberOfOutputUsages() {

		return this.usedAsOutput.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof File)) {
			return false;
		}

		final File file = (File) obj;

		if (file.compressionRatio != this.compressionRatio) {
			return false;
		}

		if (file.uncompressedFileSize != this.uncompressedFileSize) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return this.compressionRatio * (int) (this.uncompressedFileSize % 17L);
	}
}
