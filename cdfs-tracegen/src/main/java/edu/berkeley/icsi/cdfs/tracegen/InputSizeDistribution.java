package edu.berkeley.icsi.cdfs.tracegen;

final class InputSizeDistribution extends AbstractDistribution {

	private final CDFSampler sampler;

	InputSizeDistribution() {
		this.sampler = CDFSampler.add(0.0, 0.0).add(32.0, 0.0001).add(KILOBYTE, 0.1).add(KILOBYTE * 32.0, 0.26)
			.add(MEGABYTE, 0.4).add(MEGABYTE * 32.0, 0.6).add(GIGABYTE, 0.7).add(GIGABYTE * 32, 0.99)
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
