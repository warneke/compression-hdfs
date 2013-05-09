package edu.berkeley.icsi.cdfs.wlgen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;

final class MapReduceWorkload {

	private final Map<String, MapReduceJob> mapReduceJobs;

	private final Set<File> inputFiles;

	private MapReduceWorkload(final LinkedHashMap<String, MapReduceJob> mapReduceJobs, final Set<File> inputFiles) {

		this.mapReduceJobs = Collections.unmodifiableMap(mapReduceJobs);
		this.inputFiles = Collections.unmodifiableSet(inputFiles);
	}

	static MapReduceWorkload reconstructFromTraces(final String inputDir, final int mapLimit, final int reduceLimit,
			final long filesizeLimit, final int jobLimit) throws IOException {

		final LinkedHashMap<String, MapReduceJob> mapReduceJobs = new LinkedHashMap<String, MapReduceJob>();
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

				int numberOfMapTasks;
				try {
					numberOfMapTasks = Integer.parseInt(fields[1]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (numberOfMapTasks > mapLimit) {
					continue;
				}

				int numberOfReduceTasks;
				try {
					numberOfReduceTasks = Integer.parseInt(fields[2]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (numberOfReduceTasks > reduceLimit) {
					continue;
				}

				long sizeOfInputData;
				try {
					sizeOfInputData = fromGB(Double.parseDouble(fields[3]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfInputData < 0L) {
					System.err.println("Skipping trace with negative input file size " + fields[3]);
					continue;
				}

				if (sizeOfInputData > filesizeLimit) {
					continue;
				}

				long sizeOfIntermediateData;
				try {
					sizeOfIntermediateData = fromGB(Double.parseDouble(fields[4]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfIntermediateData < 0L) {
					System.err.println("Skipping trace with negative intermediate file size " + fields[4]);
					continue;
				}

				long sizeOfOutputData;
				try {
					sizeOfOutputData = fromGB(Double.parseDouble(fields[5]));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfOutputData < 0L) {
					System.err.println("Skipping trace with negative output file size " + fields[5]);
					continue;
				}

				// Adjust number of mappers if necessary
				if (numberOfMapTasks > 1 && (sizeOfInputData < ConfigConstants.BLOCK_SIZE)) {
					System.err.println("Adjusting number of mappers for job " + jobID + " from " + numberOfMapTasks
						+ " to 1");
					numberOfMapTasks = 1;
				}

				// Adjust number of reducers if necessary
				if (numberOfMapTasks < numberOfReduceTasks) {
					System.err.println("Adjusting number of reducers for job " + jobID + " from " + numberOfReduceTasks
						+ " to " + numberOfMapTasks);
					numberOfReduceTasks = numberOfMapTasks;
				}

				// Find input file
				final File inputFile = FileTracker.apply(sizeOfInputData, numberOfMapTasks, true);
				inputFiles.add(inputFile);

				// Find output file
				final File outputFile = FileTracker.apply(sizeOfOutputData, numberOfMapTasks, false);

				// Extract sequence number
				final int pos = fields[0].lastIndexOf('_');
				if (pos == -1) {
					System.err.println("Cannot extract sequence number for job with ID " + jobID);
					continue;
				}

				final int sequenceNumber = Integer.parseInt(jobID.substring(pos + 1));

				final MapReduceJob mrj = new MapReduceJob(jobID, sequenceNumber, numberOfMapTasks,
					numberOfReduceTasks, inputFile, sizeOfIntermediateData, outputFile);

				mapReduceJobs.put(mrj.getJobID(), mrj);

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

		// Show usage histogram of the files
		Statistics.showUsageHistogram(inputFiles);

		try {

			br = new BufferedReader(new FileReader(inputDir + java.io.File.separator + "ReduceInputs.txt"));
			while ((line = br.readLine()) != null) {

				final String[] fields = line.split("\t");

				if (fields.length != 9) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				final MapReduceJob mrj = mapReduceJobs.get(fields[0]);
				if (mrj == null) {
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

				final double[] dataDistribution = Partition.createPartition(mrj.getNumberOfReduceTasks(),
					mrj.getSizeOfIntermediateData(), minimum, percentile10, percentile25, median, percentile75,
					percentile90, maximum);

				mrj.setDataDistribution(dataDistribution);
			}

		} finally {
			if (br != null) {
				br.close();
				br = null;
			}
		}

		return new MapReduceWorkload(mapReduceJobs, inputFiles);
	}

	Set<File> getInputFiles() {

		return this.inputFiles;
	}

	Map<String, MapReduceJob> getMapReduceJobs() {

		return this.mapReduceJobs;
	}

	private static long fromGB(final double size) {

		return Math.round(size * 1024.0 * 1024.0 * 1024.0);
	}

	private static long fromMB(final double size) {

		return Math.round(size * 1024.0 * 1024.0);
	}

}
