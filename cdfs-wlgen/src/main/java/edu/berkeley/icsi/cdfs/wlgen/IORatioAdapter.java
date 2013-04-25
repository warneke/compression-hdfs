package edu.berkeley.icsi.cdfs.wlgen;

import eu.stratosphere.pact.common.stubs.Collector;

final class IORatioAdapter {

	private final boolean reduceMode;

	private final int ratio;

	private int counter = 0;

	IORatioAdapter(final float ioRatio) {

		if (ioRatio >= 1.0f) {
			this.reduceMode = true;
			this.ratio = Math.round(ioRatio);
		} else {
			this.reduceMode = false;
			this.ratio = Math.round(1.0f / ioRatio);
		}
	}

	void collect(final FixedByteRecord record, final Collector<FixedByteRecord> out) {

		if (this.reduceMode) {
			if (++this.counter == this.ratio) {
				out.collect(record);
				this.counter = 0;
			}

		} else {
			for (int i = 0; i < this.ratio; ++i) {
				out.collect(record);
			}
		}

	}
}
