package edu.berkeley.icsi.cdfs.traces;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class File {

	private final Set<TraceJob> usedAsInput = new HashSet<TraceJob>();

	private final Set<TraceJob> usedAsOutput = new HashSet<TraceJob>();

	private final String name;

	private final long uncompressedFileSize;

	private final int compressionFactor;

	File(final long uncompressedFileSize, final int compressionFactor) {

		if (uncompressedFileSize <= 0L) {
			throw new IllegalArgumentException("Argument uncompressedFileSize must be larger than 0 but is "
				+ uncompressedFileSize);
		}

		if (compressionFactor <= 0) {
			throw new IllegalArgumentException("Argument compressionFactor must be larger than 0 but is "
				+ compressionFactor);
		}

		this.name = "file_" + uncompressedFileSize + "_" + compressionFactor;
		this.uncompressedFileSize = uncompressedFileSize;
		this.compressionFactor = compressionFactor;
	}

	public String getName() {

		return this.name;
	}

	public long getUncompressedFileSize() {

		return this.uncompressedFileSize;
	}

	public int getCompressionFactor() {

		return this.compressionFactor;
	}

	void usedAsInputBy(final TraceJob traceJob) {

		this.usedAsInput.add(traceJob);
	}

	void usedAsOutputBy(final TraceJob traceJob) {

		this.usedAsOutput.add(traceJob);
	}

	Iterator<TraceJob> inputIterator() {

		return this.usedAsInput.iterator();
	}

	Iterator<TraceJob> outputIterator() {

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

		if (file.compressionFactor != this.compressionFactor) {
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

		return this.compressionFactor * (int) (this.uncompressedFileSize % 17L);
	}
}
