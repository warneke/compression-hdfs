package edu.berkeley.icsi.cdfs.tracegen;

final class ShuffleSizeDistribution extends AbstractDistribution {

	private final CDFSampler sampler;

	ShuffleSizeDistribution() {
		this.sampler = CDFSampler.add(0.0, 0.0).add(32.0, 0.4).add(KILOBYTE, 0.5).add(KILOBYTE * 32.0, 0.61)
			.add(MEGABYTE, 0.7).add(MEGABYTE * 32.0, 0.78).add(GIGABYTE, 0.9).add(GIGABYTE * 32.0, 0.99)
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
