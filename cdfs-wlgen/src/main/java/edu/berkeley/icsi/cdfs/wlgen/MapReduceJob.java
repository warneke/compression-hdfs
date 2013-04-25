package edu.berkeley.icsi.cdfs.wlgen;

final class MapReduceJob {

	private final String jobID;

	private final int sequenceNumber;

	private final int numberOfMapTasks;

	private final int numberOfReduceTasks;

	private final File inputFile;

	private final long sizeOfIntermediateData;

	private final File outputFile;

	private double[] dataDistribution;

	MapReduceJob(final String jobID, final int sequenceNumber, final int numberOfMapTasks,
			final int numberOfReduceTasks, final File inputFile, final long sizeOfIntermediateData,
			final File outputFile) {

		this.jobID = jobID;
		this.sequenceNumber = sequenceNumber;
		this.numberOfMapTasks = numberOfMapTasks;
		this.numberOfReduceTasks = numberOfReduceTasks;
		this.inputFile = inputFile;
		this.sizeOfIntermediateData = sizeOfIntermediateData;
		this.outputFile = outputFile;

		this.inputFile.usedAsInputBy(this);
		this.outputFile.usedAsOutputBy(this);
	}

	void setDataDistribution(double[] dataDistribution) {
		this.dataDistribution = dataDistribution;
	}

	double[] getDataDistribution() {
		return this.dataDistribution;
	}

	String getJobID() {

		return this.jobID;
	}

	int getNumberOfMapTasks() {

		return this.numberOfMapTasks;
	}

	int getNumberOfReduceTasks() {

		return this.numberOfReduceTasks;
	}

	File getInputFile() {

		return this.inputFile;
	}

	long getSizeOfIntermediateData() {

		return this.sizeOfIntermediateData;
	}

	File getOutputFile() {

		return this.outputFile;
	}
}
