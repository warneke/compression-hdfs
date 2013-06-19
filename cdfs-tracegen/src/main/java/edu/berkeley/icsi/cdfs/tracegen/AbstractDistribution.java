package edu.berkeley.icsi.cdfs.tracegen;

abstract class AbstractDistribution {

	protected static final int RESOLUTION = 100000;

	protected static final double KILOBYTE = 1024.0;

	protected static final double MEGABYTE = KILOBYTE * 1024.0;

	protected static final double GIGABYTE = MEGABYTE * 1024.0;

	protected static final double TERABYTE = GIGABYTE * 1024.0;

	abstract double sample();
}
