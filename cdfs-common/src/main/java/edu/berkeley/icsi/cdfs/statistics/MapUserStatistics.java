package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public final class MapUserStatistics extends AbstractUserStatistics {

	private PathWrapper inputFile;

	private int blockIndex;

	public MapUserStatistics(final String jobID, final int taskID, final long startTime, final long endTime,
			final Path inputFile, final int blockIndex) {
		super(jobID, taskID, startTime, endTime);

		this.inputFile = new PathWrapper(inputFile);
		this.blockIndex = blockIndex;
	}

	public MapUserStatistics() {
	}

	public Path getInputFile() {
		return this.inputFile.getPath();
	}

	public int getBlockIndex() {
		return this.blockIndex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		super.readFields(arg0);
		this.inputFile = new PathWrapper();
		this.inputFile.readFields(arg0);
		this.blockIndex = arg0.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		super.write(arg0);
		this.inputFile.write(arg0);
		arg0.writeInt(this.blockIndex);
	}
}
