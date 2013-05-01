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
		long totalAmountOfData = 0L;
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
			totalAmountOfData += file.getUncompressedFileSize();
		}

		final Iterator<Map.Entry<Integer, HistogramEntry>> it2 = histogram.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<Integer, HistogramEntry> entry = it2.next();
			final HistogramEntry he = entry.getValue();
			final double dataInGB = ((double) he.totalAmountOfData/ TO_GB);
			final float percent = (float) he.totalAmountOfData / (float) totalAmountOfData * 100.0f;
			System.out.println(entry.getKey() + ":\t" + he.numberOfFiles + " (" +dataInGB + ", " + percent + "%)");

		}
		
		System.out.println("Total amount of data generated: " + ((double) totalAmountOfData/ TO_GB) + " GB");
	}
}
