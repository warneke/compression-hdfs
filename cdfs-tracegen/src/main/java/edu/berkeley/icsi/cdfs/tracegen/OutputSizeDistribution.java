package edu.berkeley.icsi.cdfs.tracegen;

final class OutputSizeDistribution extends AbstractDistribution {

	private final CDFSampler sampler;

	OutputSizeDistribution() {
		this.sampler = CDFSampler.add(0.0, 0.0).add(32.0, 0.02).add(KILOBYTE, 0.25).add(KILOBYTE * 32.0, 0.4)
			.add(MEGABYTE, 0.65).add(MEGABYTE * 32.0, 0.8).add(GIGABYTE, 0.9).add(GIGABYTE * 32, 0.98)
			.add(TERABYTE, 1.0).finish(RESOLUTION, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	double sample() {

		return this.sampler.sample();
	}
}
