package edu.berkeley.icsi.cdfs.traces;

public class TraceJob {

	private final String jobID;

	private final int numberOfMapTasks;

	private final int numberOfReduceTasks;

	private final File inputFile;

	private final long sizeOfIntermediateData;

	private final File outputFile;

	private double[] dataDistribution;

	TraceJob(final String jobID, final int numberOfMapTasks, final int numberOfReduceTasks,
			final File inputFile, final long sizeOfIntermediateData, final File outputFile) {

		this.jobID = jobID;
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

	public double[] getDataDistribution() {
		return this.dataDistribution;
	}

	public String getJobID() {

		return this.jobID;
	}

	public int getNumberOfMapTasks() {

		return this.numberOfMapTasks;
	}

	public int getNumberOfReduceTasks() {

		return this.numberOfReduceTasks;
	}

	public File getInputFile() {

		return this.inputFile;
	}

	public long getSizeOfIntermediateData() {

		return this.sizeOfIntermediateData;
	}

	public File getOutputFile() {

		return this.outputFile;
	}
}
