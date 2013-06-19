package edu.berkeley.icsi.cdfs.tracegen;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

final class CDFSampler {

	private static final class CDFSamplerEntry {

		private final double x;

		private final double y;

		private CDFSamplerEntry(final double x, final double y) {
			this.x = x;
			this.y = y;
		}
	}

	static final class CDFSSamplerConstructor {

		private final List<CDFSamplerEntry> entries = new ArrayList<CDFSamplerEntry>();

		CDFSSamplerConstructor add(final double x, final double y) {

			this.entries.add(new CDFSamplerEntry(x, y));

			return this;
		}

		CDFSampler finish(final int resolution, final boolean fitLinear) {

			return new CDFSampler(this.entries, resolution, fitLinear);
		}
	}

	private final Random rand;

	private final double[] fields;

	static CDFSSamplerConstructor add(final double x, final double y) {

		return new CDFSSamplerConstructor().add(x, y);
	}

	private CDFSampler(final List<CDFSamplerEntry> entries, final int resolution, final boolean fitLinear) {

		this.rand = new Random();
		this.fields = new double[resolution];
		populateFields(entries, fitLinear);
	}

	private void populateFields(final List<CDFSamplerEntry> entries, final boolean fitLinear) {

		for (int i = 0; i < this.fields.length; ++i) {
			this.fields[i] = -1.0;
		}

		final double unit = this.fields.length;

		final Iterator<CDFSamplerEntry> it = entries.iterator();
		while (it.hasNext()) {

			final CDFSamplerEntry entry = it.next();
			final int index = Math.min((int) Math.floor(unit * entry.y), this.fields.length - 1);
			this.fields[index] = entry.x;
		}

		int start = 0;
		int end = -1;
		while (true) {

			end = findNextEnd(this.fields, start + 1);
			if (end == -1) {
				break;
			}

			final int len = end - start;
			if (fitLinear) {
				final double step = (this.fields[end] - this.fields[start]) / len;
				if (step < 0L) {
					throw new IllegalStateException("Calculated step size is " + step);
				}

				for (int i = 1; i < len; ++i) {
					this.fields[start + i] = this.fields[start] + (i * step);
				}
			} else {
				final double exp = Math.log(this.fields[end] - this.fields[start]) / (Math.log(len));
				// System.out.println("Exponent " + exp);

				for (int i = 1; i < len; ++i) {
					this.fields[start + i] = this.fields[start] + Math.pow(i, exp);
				}
			}

			start = end;
		}
	}

	private static int findNextEnd(final double[] fields, final int start) {

		for (int i = start; i < fields.length; ++i) {

			if (fields[i] >= 0.0) {
				return i;
			}
		}

		return -1;
	}

	double sample() {

		return this.fields[this.rand.nextInt(this.fields.length)];
	}
}
