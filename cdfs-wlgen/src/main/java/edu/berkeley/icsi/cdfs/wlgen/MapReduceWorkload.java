package edu.berkeley.icsi.cdfs.wlgen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final class MapReduceWorkload {

	private final Map<String, MapReduceJob> mapReduceJobs;

	private final Map<Long, File> inputFiles;

	private MapReduceWorkload(final LinkedHashMap<String, MapReduceJob> mapReduceJobs, final Map<Long, File> inputFiles) {

		this.mapReduceJobs = Collections.unmodifiableMap(mapReduceJobs);
		this.inputFiles = Collections.unmodifiableMap(inputFiles);
	}

	static MapReduceWorkload reconstructFromTraces(final String inputDir, final int mapLimit, final int reduceLimit,
			final long filesizeLimit, final int jobLimit) throws IOException {

		final LinkedHashMap<String, MapReduceJob> mapReduceJobs = new LinkedHashMap<String, MapReduceJob>();
		final Map<Long, File> files = new HashMap<Long, File>();
		final Map<Long, File> inputFiles = new HashMap<Long, File>();
		BufferedReader br = null;
		String line;

		try {

			br = new BufferedReader(new FileReader("/home/stratosphere/wlgen/JobStats.txt"));

			int count = 0;
			while ((line = br.readLine()) != null) {

				final String[] fields = line.split("\t");

				if (fields.length != 6) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

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

				Long sizeOfInputData;
				try {
					sizeOfInputData = Long.valueOf(fromGB(Double.parseDouble(fields[3])));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfInputData.longValue() < 0L) {
					System.err.println("Skipping trace with negative input file size " + fields[3]);
					continue;
				}

				if (sizeOfInputData.longValue() > filesizeLimit) {
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

				Long sizeOfOutputData;
				try {
					sizeOfOutputData = Long.valueOf(fromGB(Double.parseDouble(fields[5])));
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (sizeOfOutputData.longValue() < 0L) {
					System.err.println("Skipping trace with negative output file size " + fields[5]);
					continue;
				}

				// Find input file
				File inputFile = files.get(sizeOfInputData);
				if (inputFile == null) {
					inputFile = new File(sizeOfInputData.longValue());
					files.put(sizeOfInputData, inputFile);
					inputFiles.put(sizeOfInputData, inputFile);
				}

				// Find output file
				File outputFile = files.get(sizeOfOutputData);
				if (outputFile == null) {
					outputFile = new File(sizeOfOutputData.longValue());
					files.put(sizeOfOutputData, outputFile);
				}

				// Extract sequence number
				final int pos = fields[0].lastIndexOf('_');
				if (pos == -1) {
					System.err.println("Cannot extract sequence number for job with ID " + fields[0]);
					continue;
				}

				final int sequenceNumber = Integer.parseInt(fields[0].substring(pos + 1));

				final MapReduceJob mrj = new MapReduceJob(fields[0], sequenceNumber, numberOfMapTasks,
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

		try {

			br = new BufferedReader(new FileReader("/home/stratosphere/wlgen/ReduceInputs.txt"));
			while ((line = br.readLine()) != null) {

				final String[] fields = line.split("\t");

				System.out.println("READ " + line);
				
				if (fields.length != 9) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				final MapReduceJob mrj = mapReduceJobs.get(fields[0]);
				if (mrj == null) {
					continue;
				}

				int numberOfReduceTasks;
				try {
					numberOfReduceTasks = Integer.parseInt(fields[1]);
				} catch (NumberFormatException nfe) {
					System.err.println("Cannot parse trace '" + line + "', skipping it...");
					continue;
				}

				if (numberOfReduceTasks != mrj.getNumberOfReduceTasks()) {
					System.err.println("Number of reduce tasks does not match for job " + mrj.getJobID());
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

		// findDependencies(mapReduceJobs);

		return new MapReduceWorkload(mapReduceJobs, inputFiles);
	}

	Map<Long, File> getInputFiles() {

		return this.inputFiles;
	}

	Map<String, MapReduceJob> getMapReduceJobs() {

		return this.mapReduceJobs;
	}

	private static void findDependencies(final LinkedHashMap<String, MapReduceJob> mapReduceJobs) {

		final Set<MapReduceJob> alreadyVisited = new HashSet<MapReduceJob>();

		final Iterator<MapReduceJob> it = mapReduceJobs.values().iterator();
		while (it.hasNext()) {
			findDependencies(it.next(), alreadyVisited);
			System.out.println("");
		}
	}

	private static void findDependencies(final MapReduceJob mapReduceJob, final Set<MapReduceJob> alreadyVisited) {

		if (!alreadyVisited.add(mapReduceJob)) {
			return;
		}

		final File outputFile = mapReduceJob.getOutputFile();

		final Iterator<MapReduceJob> it = outputFile.inputIterator();

		System.out.print(mapReduceJob.getJobID());

		while (it.hasNext()) {
			System.out.print(" -> ");
			findDependencies(it.next(), alreadyVisited);
			System.out.println("");
		}

	}

	private static long fromGB(final double size) {

		return Math.round(size * 1024.0 * 1024.0 * 1024.0);
	}

	private static long fromMB(final double size) {

		return Math.round(size * 1024.0 * 1024.0);
	}

}
