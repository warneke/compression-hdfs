package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;

public final class RemoteJobRunner {

	private static final long SLEEP_TIME = 5000;

	private RemoteJobRunner() {
	}

	public static void submitAndWait(final List<Job> jobs) throws IOException, InterruptedException,
			ClassNotFoundException {

		final List<Job> runningJobs = new LinkedList<Job>();

		for (final Job job : jobs) {
			job.submit();
			runningJobs.add(job);
		}

		while (!runningJobs.isEmpty()) {

			System.out.println("Job progress:");
			final Iterator<Job> it = runningJobs.iterator();
			while (it.hasNext()) {

				final Job job = it.next();
				if (job.isComplete()) {
					it.remove();
					continue;
				}

				System.out.println(job.getJobName() + " " + getProgressBar(job.mapProgress(), job.reduceProgress()));
			}

			Thread.sleep(SLEEP_TIME);
		}
	}

	private static String getProgressBar(final float mapProgress, final float reduceProgress) {

		final StringBuilder sb = new StringBuilder(104);

		sb.append('[');
		final int mp = Math.round(mapProgress / 2.0f);
		final int rp = Math.round(reduceProgress / 2.0f);

		for (int i = 0; i < 50; ++i) {
			if (i < mp) {
				sb.append('M');
			} else {
				sb.append(' ');
			}
		}

		sb.append(']');
		sb.append('[');

		for (int i = 0; i < 50; ++i) {
			if (i < rp) {
				sb.append('R');
			} else {
				sb.append(' ');
			}
		}

		sb.append(']');

		return sb.toString();
	}
}
