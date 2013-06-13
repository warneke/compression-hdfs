package edu.berkeley.icsi.cdfs.traces;

final class Partition {

	static double[] createPartition(final int numberOfReducers, final long sizeOfIntermediateInput, final long minimum,
			final long percentile10, final long percentile25, final long median, final long percentile75,
			final long percentile90, final long maximum) {

		final long partition[] = new long[numberOfReducers];

		for (int i = 0; i < numberOfReducers; ++i) {
			partition[i] = -1L;
		}

		partition[0] = minimum;
		partition[toIndex(numberOfReducers, 10)] = percentile10;
		partition[toIndex(numberOfReducers, 25)] = percentile25;
		partition[toIndex(numberOfReducers, 50)] = median;
		partition[toIndex(numberOfReducers, 75)] = percentile75;
		partition[toIndex(numberOfReducers, 90)] = percentile90;
		partition[numberOfReducers - 1] = maximum;

		int start = 0;
		int end = -1;
		while (true) {

			end = findNextEnd(partition, start + 1);
			if (end == -1) {
				break;
			}

			final int len = end - start;
			final long step = (partition[end] - partition[start]) / len;
			if (step < 0L) {
				throw new IllegalStateException("Calculated step size is " + step);
			}

			for (int i = 1; i < len; ++i) {
				partition[start + i] = partition[start] + (i * step);
			}

			start = end;
		}

		return normalize(partition);
	}

	private static double[] normalize(final long[] input) {

		double total = 0.0;
		for (int i = 0; i < input.length; ++i) {
			total += input[i];
		}

		double sum = 0.0;
		final double[] output = new double[input.length - 1];
		for (int i = 0; i < output.length; ++i) {
			output[i] = +(double) ((sum + input[i]) / total);
			sum += input[i];
		}

		return output;
	}

	private static int findNextEnd(final long[] partition, final int start) {

		for (int i = start; i < partition.length; ++i) {

			if (partition[i] != -1L) {
				return i;
			}
		}

		return -1;
	}

	private static int toIndex(final int arrayLength, final int percentile) {

		final double unit = arrayLength / 100.0;

		return (int) Math.floor(unit * percentile);
	}

	static void printPartition(final long[] partition) {

		System.out.println("====================================================================================");
		for (int i = 0; i < partition.length; ++i) {

			if (partition[i] != -1L) {
				System.out.print(partition[i]);
			}
			if (i < (partition.length - 1)) {
				System.out.print(":");
			}

		}
		System.out.println("");
	}
}
