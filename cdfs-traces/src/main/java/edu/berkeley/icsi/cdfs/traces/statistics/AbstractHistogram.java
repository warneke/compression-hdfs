package edu.berkeley.icsi.cdfs.traces.statistics;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public abstract class AbstractHistogram<T extends Number> {

	private final TreeMap<T, Integer> histogram = new TreeMap<T, Integer>();

	private final TreeMap<Integer, Integer> bins = new TreeMap<Integer, Integer>();

	public void add(final T k) {

		Integer val = this.histogram.get(k);
		if (val == null) {
			val = Integer.valueOf(1);
		} else {
			val = Integer.valueOf(val.intValue() + 1);
		}
		this.histogram.put(k, val);

		final Integer bin = toBin(k);
		val = this.bins.get(bin);
		if (val == null) {
			val = Integer.valueOf(1);
		} else {
			val = Integer.valueOf(val.intValue() + 1);
		}
		this.bins.put(bin, val);
	}

	private Integer toBin(final T k) {

		final int bin = (int) Math.ceil(Math.log10(k.doubleValue()));

		return Integer.valueOf(bin);
	}

	public void showBins() {

		final StringBuilder sb = new StringBuilder();

		for (final Iterator<Map.Entry<Integer, Integer>> it = this.bins.entrySet().iterator(); it.hasNext();) {

			final Map.Entry<Integer, Integer> entry = it.next();
			sb.append('\n');
			sb.append(entry.getKey().toString());
			sb.append('\t');
			sb.append(entry.getValue().toString());
		}

		System.out.println(sb.toString());
	}

	public void showHistogram() {

		final StringBuilder sb = new StringBuilder();

		for (final Iterator<Map.Entry<T, Integer>> it = this.histogram.entrySet().iterator(); it.hasNext();) {

			final Map.Entry<T, Integer> entry = it.next();
			sb.append('\n');
			sb.append(entry.getKey().toString());
			sb.append('\t');
			sb.append(entry.getValue().toString());
		}

		System.out.println(sb.toString());
	}
}
