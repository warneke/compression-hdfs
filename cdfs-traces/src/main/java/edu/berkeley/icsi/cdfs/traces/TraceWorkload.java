package edu.berkeley.icsi.cdfs.traces;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;

public final class TraceWorkload {

	public static final long FILE_GRANULARITY = 8L * 1024L * 1024L; // 4 KB

	private final Map<String, TraceJob> traceJobs;

	private final Set<File> inputFiles;

	private TraceWorkload(final LinkedHashMap<String, TraceJob> traceJobs, final Set<File> inputFiles) {

		this.traceJobs = Collections.unmodifiableMap(traceJobs);
		this.inputFiles = Collections.unmodifiableSet(inputFiles);
	}

	public static TraceWorkload reconstructFromFiles(final String inputDir, final int mapLimit, final int reduceLimit,
			final long filesizeLimit, final int jobLimit) throws IOException {

		final LinkedHashMap<String, TraceJob> traceJobs = new LinkedHashMap<String, TraceJob>();
		BufferedReader br = null;
		String line;

		final Set<File> inputFiles = new HashSet<File>();

		try {

			br = new BufferedReader(new FileReader(inputDir + java.io.File.separator + "JobStats.txt"));

			int count = 0;
			while ((line = br.readLine()) != null) {

				final String[] fields = line.split("\t");

				if (fields.length != 6) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				final String jobID = fields[0];

				final int numberOfMapTasks;
				try {
					numberOfMapTasks = Integer.parseInt(fields[1]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (numberOfMapTasks > mapLimit) {
					continue;
				}

				final int numberOfReduceTasks;
				try {
					numberOfReduceTasks = Integer.parseInt(fields[2]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (numberOfReduceTasks > reduceLimit) {
					continue;
				}

				final long sizeOfInputData;
				try {
					sizeOfInputData = fromGB(Double.parseDouble(fields[3]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfInputData <= FILE_GRANULARITY) {
					System.err.println("Skipping trace with input file size " + fields[3]);
					continue;
				}

				if (sizeOfInputData > filesizeLimit) {
					continue;
				}

				final long sizeOfIntermediateData;
				try {
					sizeOfIntermediateData = fromGB(Double.parseDouble(fields[4]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfIntermediateData <= 0L) {
					System.err.println("Skipping trace with non-positive intermediate file size " + fields[4]);
					continue;
				}

				final long sizeOfOutputData;
				try {
					sizeOfOutputData = fromGB(Double.parseDouble(fields[5]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfOutputData <= 0L) {
					System.err.println("Skipping trace with non-positive output file size " + fields[5]);
					continue;
				}

				// Check if number of mappers makes sense
				if (numberOfMapTasks > 1 && (sizeOfInputData < ConfigConstants.BLOCK_SIZE)) {
					System.err.println("Skipping job " + jobID + " because it specifies " + numberOfMapTasks
						+ " mappers and an input size of " + (sizeOfInputData / 1024L) + " KB");
					continue;
				}

				// Adjust number of reducers if necessary
				if (numberOfMapTasks < numberOfReduceTasks) {
					System.err.println("Skipping job " + jobID + " because it specifies only " + numberOfMapTasks
						+ " mappers but " + numberOfReduceTasks + " reducers");
					continue;
				}

				// Find input file
				final File inputFile = FileTracker.apply(sizeOfInputData, numberOfMapTasks, true);
				inputFiles.add(inputFile);

				// Find output file
				final File outputFile = FileTracker.apply(sizeOfOutputData, numberOfMapTasks, false);

				final TraceJob tj = new TraceJob(jobID, numberOfMapTasks, numberOfReduceTasks, inputFile,
					sizeOfIntermediateData, outputFile);

				traceJobs.put(tj.getJobID(), tj);

				if (++count >= jobLimit) {
					break;
				}
			}

		} finally {
			if (br != null) {
				br.close();
				br = null;
			}
		}

		try {

			br = new BufferedReader(new FileReader(inputDir + java.io.File.separator + "ReduceInputs.txt"));
			while ((line = br.readLine()) != null) {

				final String[] fields = line.split("\t");

				if (fields.length != 9) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				final TraceJob tj = traceJobs.get(fields[0]);
				if (tj == null) {
					continue;
				}

				long minimum;
				try {
					minimum = fromMB(Double.parseDouble(fields[2]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				long percentile10;
				try {
					percentile10 = fromMB(Double.parseDouble(fields[3]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				long percentile25;
				try {
					percentile25 = fromMB(Double.parseDouble(fields[4]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				long median;
				try {
					median = fromMB(Double.parseDouble(fields[5]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				long percentile75;
				try {
					percentile75 = fromMB(Double.parseDouble(fields[6]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				long percentile90;
				try {
					percentile90 = fromMB(Double.parseDouble(fields[7]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				long maximum;
				try {
					maximum = fromMB(Double.parseDouble(fields[8]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				final double[] dataDistribution = Partition.createPartition(tj.getNumberOfReduceTasks(),
					tj.getSizeOfIntermediateData(), minimum, percentile10, percentile25, median, percentile75,
					percentile90, maximum);

				tj.setDataDistribution(dataDistribution);
			}

		} finally {
			if (br != null) {
				br.close();
				br = null;
			}
		}

		return new TraceWorkload(traceJobs, inputFiles);
	}

	public Set<File> getInputFiles() {

		return this.inputFiles;
	}

	public Map<String, TraceJob> getMapReduceJobs() {

		return this.traceJobs;
	}

	private static long fromGB(final double size) {

		return Math.round(size * 1024.0 * 1024.0 * 1024.0);
	}

	private static long fromMB(final double size) {

		return Math.round(size * 1024.0 * 1024.0);
	}

}
