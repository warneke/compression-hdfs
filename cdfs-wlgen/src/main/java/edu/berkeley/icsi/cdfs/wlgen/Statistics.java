package edu.berkeley.icsi.cdfs.wlgen;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public final class Statistics {

	private static final double TO_GB = 1024.0 * 1024.0 * 1024.0;

	private static final class HistogramEntry {

		private int numberOfFiles = 0;

		private long totalAmountOfData = 0L;
	}

	private Statistics() {
	}

	public static void showUsageHistogram(final Collection<File> files) {

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

		final Iterator<Map.Entry<Integer, HistogramEntry>> it2 = histogram.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<Integer, HistogramEntry> entry = it2.next();
			final HistogramEntry he = entry.getValue();
			final float generatedAmountInGB = (float) ((double) he.totalAmountOfData / TO_GB);
			final float percentGeneratedData = (float) he.totalAmountOfData / (float) totalAmountOfGeneratedData
				* 100.0f;
			final float readAmountInGB = (float) ((double) he.totalAmountOfData * entry.getKey().intValue() / TO_GB);
			final float percentReadData = (float) he.totalAmountOfData * entry.getKey().intValue()
				/ (float) totalAmountOfReadData * 100.0f;
			printLine(entry.getKey().intValue(), he.numberOfFiles, generatedAmountInGB, percentGeneratedData,
				readAmountInGB, percentReadData);

		}

		final float totalGeneratedInGB = (float) ((double) totalAmountOfGeneratedData / TO_GB);
		final float totalReadInGB = (float) ((double) totalAmountOfReadData / TO_GB);
		System.out.println("Total generated " + String.format("%.2f", totalGeneratedInGB) + " GB, total read "
			+ String.format("%.2f", totalReadInGB) + " GB");
	}

	private static void printLine(final int numberOfInputUsages, final int numberOfIndividualFiles,
			final float amountOfGeneratedDataInGB, final float percentGeneratedData, final float amountOfReadDataInGB,
			final float percentReadData) {

		final StringBuffer sb = new StringBuffer();

		sb.append(padString(Integer.toString(numberOfInputUsages), 5));
		sb.append('|');
		sb.append(padString(Integer.toString(numberOfIndividualFiles), 5));
		sb.append('|');
		sb.append(padString(String.format("%.2f", amountOfGeneratedDataInGB), 9));
		sb.append('|');
		sb.append(padString(String.format("%.1f", percentGeneratedData), 5));
		sb.append('%');
		sb.append('|');
		sb.append(padString(String.format("%.2f", amountOfReadDataInGB), 9));
		sb.append('|');
		sb.append(padString(String.format("%.1f", percentReadData), 5));
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
