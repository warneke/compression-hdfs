package edu.berkeley.icsi.cdfs.traces;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

final class ReadStatistics {

	private static final double TO_GB = 1024.0 * 1024.0 * 1024.0;

	private static final class HistogramEntry {

		private int numberOfFiles = 0;

		private long totalAmountOfData = 0L;
	}

	private ReadStatistics() {
	}

	static void showUsageHistogram(final Collection<File> files) {

		System.out.println("Total number of files: " + files.size());

		final Map<Integer, HistogramEntry> histogram = new TreeMap<Integer, HistogramEntry>();
		final Iterator<File> it = files.iterator();
		long totalAmountOfGeneratedData = 0L;
		long totalAmountOfReadData = 0L;
		while (it.hasNext()) {

			final File file = it.next();
			final Integer numberOfInputUsages = Integer.valueOf(file.getNumberOfInputUsages());
			if (numberOfInputUsages.intValue() == 0) {
				continue;
			}

			HistogramEntry entry = histogram.get(numberOfInputUsages);
			if (entry == null) {
				entry = new HistogramEntry();
				histogram.put(numberOfInputUsages, entry);
			}
			++entry.numberOfFiles;
			entry.totalAmountOfData += file.getUncompressedFileSize();
			totalAmountOfGeneratedData += file.getUncompressedFileSize();
			totalAmountOfReadData += (file.getUncompressedFileSize() * file.getNumberOfInputUsages());
		}

		System.out.println("#reads|#files|dgen(GB)|dgen(%)|dread(GB)|dread(%)|");
		final Iterator<Map.Entry<Integer, HistogramEntry>> it2 = histogram.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<Integer, HistogramEntry> entry = it2.next();
			final HistogramEntry he = entry.getValue();
			final double generatedAmountInGB = (double) he.totalAmountOfData / TO_GB;
			final double percentGeneratedData = (double) he.totalAmountOfData / (double) totalAmountOfGeneratedData
				* 100.0;
			final double readAmountInGB = (double) he.totalAmountOfData * entry.getKey().intValue() / TO_GB;
			final double percentReadData = (double) he.totalAmountOfData * (double) entry.getKey().intValue()
				/ (double) totalAmountOfReadData * 100.0;
			printLine(entry.getKey().intValue(), he.numberOfFiles, generatedAmountInGB, percentGeneratedData,
				readAmountInGB, percentReadData);

		}

		final float totalGeneratedInGB = (float) ((double) totalAmountOfGeneratedData / TO_GB);
		final float totalReadInGB = (float) ((double) totalAmountOfReadData / TO_GB);
		System.out.println("Total generated " + String.format("%.2f", totalGeneratedInGB) + " GB, total read "
			+ String.format("%.2f", totalReadInGB) + " GB");
	}

	private static void printLine(final int numberOfInputUsages, final int numberOfIndividualFiles,
			final double amountOfGeneratedDataInGB, final double percentGeneratedData,
			final double amountOfReadDataInGB, final double percentReadData) {

		final StringBuffer sb = new StringBuffer();

		sb.append(padString(Integer.toString(numberOfInputUsages), 6));
		sb.append('|');
		sb.append(padString(Integer.toString(numberOfIndividualFiles), 6));
		sb.append('|');
		sb.append(padString(String.format("%.2f", amountOfGeneratedDataInGB), 8));
		sb.append('|');
		sb.append(padString(String.format("%.1f", percentGeneratedData), 6));
		sb.append('%');
		sb.append('|');
		sb.append(padString(String.format("%.2f", amountOfReadDataInGB), 9));
		sb.append('|');
		sb.append(padString(String.format("%.1f", percentReadData), 7));
		sb.append('%');
		sb.append('|');

		System.out.println(sb.toString());
	}

	private static String padString(final String str, final int width) {

		final StringBuffer sb = new StringBuffer(Math.max(str.length(), width));

		int pad = width - str.length();
		while (pad > 0) {

			sb.append(' ');

			--pad;
		}

		sb.append(str);

		return sb.toString();
	}
}
