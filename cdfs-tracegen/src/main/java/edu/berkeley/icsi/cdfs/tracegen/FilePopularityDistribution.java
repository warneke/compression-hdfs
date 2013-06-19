package edu.berkeley.icsi.cdfs.tracegen;

import org.apache.commons.math3.distribution.ZipfDistribution;

final class FilePopularityDistribution {

	private static final double EXPONENT = 0.93979;

	private final ZipfDistribution zipf;

	FilePopularityDistribution(final int numberOfFiles) {

		this.zipf = new ZipfDistribution(numberOfFiles, EXPONENT);
	}

	int sample() {

		return this.zipf.sample();
	}
}
