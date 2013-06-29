package edu.berkeley.icsi.cdfs.tracegen;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public final class TraceGenerator {

	private static final int MAX_COMPRESSION_RATIO = 10;

	private static final int NUMBER_OF_FILES = 200;

	private static final int BLOCK_SIZE = 256 * 1024 * 1024;

	private static final int REDUCE_LIMIT = 128 * 1024 * 1024;

	public static void main(final String[] args) throws Exception {

		final InputSizeDistribution inputSize = new InputSizeDistribution();
		final ShuffleSizeDistribution shuffleSize = new ShuffleSizeDistribution();
		final OutputSizeDistribution outputSize = new OutputSizeDistribution();
		final Random compressionRand = new Random();
		final FilePopularityDistribution fpd = new FilePopularityDistribution(NUMBER_OF_FILES);

		final ArrayList<File> inputFiles = new ArrayList<File>(NUMBER_OF_FILES);

		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/warneke/wlgen2/files.txt"));

		for (int i = 0; i < NUMBER_OF_FILES; ++i) {
			final long uncompressedFileSize = (long) inputSize.sample();
			// final int compressionFactor = compressionRand.nextInt(MAX_COMPRESSION_RATIO) + 1;
			final File file = new File(uncompressedFileSize, 2);
			inputFiles.add(file);

			final StringBuilder sb = new StringBuilder(file.toString());
			sb.append('\t');
			sb.append(file.getUncompressedFileSize());
			sb.append('\t');
			sb.append(file.getCompressionFactor());
			sb.append('\n');
			bw.write(sb.toString());
		}

		bw.close();

		// Adjust popularity
		PopularityShifter.adjust(inputFiles, fpd);

		final Set<File> unaccessedFiles = new HashSet<File>();
		unaccessedFiles.addAll(inputFiles);

		final LinkedList<Long> shuffleSetAsideList = new LinkedList<Long>();
		final LinkedList<Long> outputSetAsideList = new LinkedList<Long>();

		final List<File> jobSequence = new ArrayList<File>();

		int count = 0;
		bw = new BufferedWriter(new FileWriter("/home/warneke/wlgen2/jobs.txt"));

		while (!unaccessedFiles.isEmpty()) {

			// Generate jobs
			final File inputFile = inputFiles.get(fpd.sample() - 1);
			unaccessedFiles.remove(inputFile);

			jobSequence.add(inputFile);

			// Generate job ID
			final String jobID = String.format("job_%06d", count++);

			// Compute number of required map tasks
			final int numberOfMapTasks = (int) Math.ceil((double) inputFile.getUncompressedFileSize()
				/ (double) BLOCK_SIZE);

			long shuffle = -1L;

			while (true) {

				// Make sure to use the numbers we set aside previously
				if (!shuffleSetAsideList.isEmpty()) {
					final Iterator<Long> it = shuffleSetAsideList.iterator();
					while (it.hasNext()) {
						final long cand = it.next().longValue();
						if (cand <= inputFile.getUncompressedFileSize()) {
							it.remove();
							shuffle = cand;
							break;
						}
					}
				}

				if (shuffle != -1L) {
					break;
				}

				shuffle = (long) shuffleSize.sample();
				if (shuffle <= inputFile.getUncompressedFileSize()) {
					break;
				} else {
					shuffleSetAsideList.add(Long.valueOf(shuffle));
					shuffle = -1L;
				}
			}

			final int numberOfReduceTasks;
			if (shuffle < 100L) { // Data to transport is less than one record
				numberOfReduceTasks = 0;
			} else {
				numberOfReduceTasks = Math.min((int) Math.ceil((double) shuffle / (double) REDUCE_LIMIT),
					numberOfMapTasks);
			}

			long output = -1L;

			while (true) {

				// Make sure to use the numbers we set aside previously
				if (!outputSetAsideList.isEmpty()) {
					final Iterator<Long> it = outputSetAsideList.iterator();
					while (it.hasNext()) {
						final long cand = it.next().longValue();
						if (cand <= inputFile.getUncompressedFileSize()) {
							it.remove();
							output = cand;
							break;
						}
					}
				}

				if (output != -1L) {
					break;
				}

				output = (long) outputSize.sample();
				if (output <= inputFile.getUncompressedFileSize()) {
					break;
				} else {
					outputSetAsideList.add(Long.valueOf(output));
					output = -1L;
				}
			}

			final StringBuilder sb = new StringBuilder(jobID);
			sb.append('\t');
			sb.append(numberOfMapTasks);
			sb.append('\t');
			sb.append(numberOfReduceTasks);
			sb.append('\t');
			sb.append(inputFile);
			sb.append('\t');
			sb.append(toGB(shuffle));
			sb.append('\t');
			sb.append(toGB(output));
			sb.append('\n');

			bw.write(sb.toString());
		}

		bw.close();
	}

	static final double toGB(final long byteValue) {

		return (double) byteValue / (1024.0 * 1024.0 * 1024.0);
	}
}
