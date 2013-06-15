package edu.berkeley.icsi.cdfs.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class CompareRuntimes {

	private static final int[] BIN_BOUNDARIES = { 10, 50, 150, 500 };

	private static final Map<Integer, Map<JobKey, Long>> BINS = initializeBins();

	private static final Map<Integer, Float> SPEEDUP_AVERAGES = new HashMap<Integer, Float>();

	private static final class JobKey {

		private final String jobID;

		private final int numberOfTasks;

		private JobKey(final String jobID, final int numberOfTasks) {
			this.jobID = jobID;
			this.numberOfTasks = numberOfTasks;
		}

		/**
		 * {@inheritDoc}
		 */
		public boolean equals(final Object obj) {

			if (!(obj instanceof JobKey)) {
				return false;
			}

			final JobKey jk = (JobKey) obj;

			if (!this.jobID.equals(jk.jobID)) {
				return false;
			}

			if (this.numberOfTasks != jk.numberOfTasks) {
				return false;
			}

			return true;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {

			return this.jobID.hashCode() + this.numberOfTasks;
		}
	}

	private static Map<Integer, Map<JobKey, Long>> initializeBins() {

		Arrays.sort(BIN_BOUNDARIES);

		final Map<Integer, Map<JobKey, Long>> maps = new HashMap<Integer, Map<JobKey, Long>>(BIN_BOUNDARIES.length + 1);

		for (int i = 0; i < BIN_BOUNDARIES.length; ++i) {
			maps.put(Integer.valueOf(BIN_BOUNDARIES[i]), new HashMap<JobKey, Long>());
		}

		maps.put(Integer.valueOf(Integer.MAX_VALUE), new HashMap<JobKey, Long>());

		return maps;
	}

	private static int numberOfTaskstoBin(final int numberOfTasks) {

		for (int i = 0; i < BIN_BOUNDARIES.length; ++i) {
			final int boundary = BIN_BOUNDARIES[i];
			if (numberOfTasks <= boundary) {
				return boundary;
			}
		}

		return Integer.MAX_VALUE;
	}

	private static Map<JobKey, Long> getBin(final int numberOfTasks) {

		return BINS.get(numberOfTaskstoBin(numberOfTasks));
	}

	private static void increaseSpeedAverage(final int numberOfTasks, final float speedUp) {

		final Integer bin = Integer.valueOf(numberOfTaskstoBin(numberOfTasks));

		Float val = SPEEDUP_AVERAGES.get(bin);
		if (val == null) {
			val = Float.valueOf(speedUp);
		} else {
			val = Float.valueOf(val.floatValue() + speedUp);
		}
		SPEEDUP_AVERAGES.put(bin, val);
	}

	private static float speedUpInPercent(final long runtime1, final long runtime2) {

		final long diff = runtime1 - runtime2;
		final double percentage = ((double) diff / (double) runtime1);

		return (float) Math.round(percentage * 1000.0) / 10.0f;
	}

	public static void main(final String[] args) {

		if (args.length < 2) {
			System.out.println("Please provide two inputs to compare");
			return;
		}

		BufferedReader br = null;
		try {

			// Read the first file
			br = new BufferedReader(new FileReader(args[0]));

			while (true) {

				final String line = br.readLine();
				if (line == null) {
					break;
				}

				final String[] fields = line.split("\\t");
				if (fields.length != 4) {
					throw new IllegalArgumentException("Input file " + args[0] + " is of unknown format");
				}

				final int numberOfTasks = Integer.parseInt(fields[1]) + Integer.parseInt(fields[2]);
				final Map<JobKey, Long> runtimes = getBin(numberOfTasks);

				runtimes.put(new JobKey(fields[0], numberOfTasks), Long.parseLong(fields[3]));
			}

			br.close();

			// Read the second file
			br = new BufferedReader(new FileReader(args[1]));

			while (true) {

				final String line = br.readLine();
				if (line == null) {
					break;
				}

				final String[] fields = line.split("\\t");
				if (fields.length != 4) {
					throw new IllegalArgumentException("Input file " + args[0] + " is of unknown format");
				}

				final int numberOfTasks = Integer.parseInt(fields[1]) + Integer.parseInt(fields[2]);

				final Map<JobKey, Long> runtimes = getBin(numberOfTasks);

				final Long rt = runtimes.get(new JobKey(fields[0], numberOfTasks));
				if (rt == null) {
					System.err.println("Job " + fields[0] + " is not included in the first file");
					continue;
				}

				final long runtime1 = rt.longValue();
				final long runtime2 = Long.parseLong(fields[3]);

				final StringBuilder sb = new StringBuilder(fields[0]);
				sb.append('\t');
				sb.append(runtime1);
				sb.append('\t');
				sb.append(runtime2);
				sb.append('\t');
				final float speedUp = speedUpInPercent(runtime1, runtime2);
				sb.append(speedUp);
				sb.append('%');

				System.out.println(sb.toString());

				increaseSpeedAverage(numberOfTasks, speedUp);
			}

			// Compute the average speed ups for every bin
			final Iterator<Map.Entry<Integer, Float>> it = SPEEDUP_AVERAGES.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<Integer, Float> entry = it.next();
				final Integer key = entry.getKey();
				final float sumOfAverages = entry.getValue().floatValue();
				System.out.println(key + ": average speed-up is " + (sumOfAverages / BINS.get(key).size()));
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
