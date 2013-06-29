package edu.berkeley.icsi.cdfs.traces.statistics;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import edu.berkeley.icsi.cdfs.traces.File;

public final class CompressionStatistics {

	public static void showCompressionHistogram(final Collection<File> files) {

		final Map<Integer, Integer> histogram = new TreeMap<Integer, Integer>();

		for (final Iterator<File> it = files.iterator(); it.hasNext();) {

			final Integer compressionFactor = Integer.valueOf(it.next().getCompressionFactor());

			Integer val = histogram.get(compressionFactor);
			if (val == null) {
				val = Integer.valueOf(1);
			} else {
				val = Integer.valueOf(val.intValue() + 1);
			}
			histogram.put(compressionFactor, val);
		}

		for (final Iterator<Map.Entry<Integer, Integer>> it = histogram.entrySet().iterator(); it.hasNext();) {

			final Map.Entry<Integer, Integer> entry = it.next();

			final StringBuilder sb = new StringBuilder(entry.getKey().toString());
			sb.append('\t');
			sb.append(entry.getValue().intValue());

			System.out.println(sb.toString());
		}
	}

}
