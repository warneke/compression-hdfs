package edu.berkeley.icsi.cdfs.traces;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

		final Map<String, File> inputFiles = new HashMap<String, File>();

		// Read the files
		try {

			br = new BufferedReader(new FileReader(inputDir + java.io.File.separator + "files.txt"));

			while ((line = br.readLine()) != null) {

				final String[] fields = line.split("\t");

				if (fields.length != 3) {
					System.err.println("Cannot parse file trace '" + line + "', skipping it...");
					continue;
				}

				final String fileName = fields[0];

				final long uncompressedFileSize;
				try {
					uncompressedFileSize = Long.parseLong(fields[1]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse file trace '" + line + "', skipping it...");
					continue;
				}

				final int compressionFactor;
				try {
					compressionFactor = Integer.parseInt(fields[2]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse file trace '" + line + "', skipping it...");
					continue;
				}

				inputFiles.put(fileName, new File(uncompressedFileSize, compressionFactor));
			}

		} finally {
			if (br != null) {
				br.close();
				br = null;
			}
		}

		try {

			br = new BufferedReader(new FileReader(inputDir + java.io.File.separator + "jobs.txt"));

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

				final String inputFileName = fields[3];

				final File inputFile = inputFiles.get(inputFileName);
				if (inputFile == null) {
					System.err.println("Cannot find input file '" + inputFileName + "', skipping it...");
					continue;
				}

				if (inputFile.getUncompressedFileSize() <= FILE_GRANULARITY) {
					System.err.println("Skipping trace with input file size " + fields[3]);
					continue;
				}

				if (inputFile.getUncompressedFileSize() > filesizeLimit) {
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

				// Find output file
				final File outputFile = new File(sizeOfOutputData, inputFile.getCompressionFactor());

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

		return new TraceWorkload(traceJobs, new HashSet<File>(inputFiles.values()));
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
}
