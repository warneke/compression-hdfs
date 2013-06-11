package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public final class RemoteJobRunner {

	private static final String JOB_SUBMISSION_TIME_KEY = "job.submission.time";

	private static final long SLEEP_TIME = 200;

	private RemoteJobRunner() {
	}

	public static void submitAndWait(final List<Job> jobs) throws IOException, InterruptedException,
			ClassNotFoundException {

		System.out.println("Launching " + jobs.size() + " jobs");

		final List<Job> runningJobs = new LinkedList<Job>();

		final Map<Job, String> progressMap = new HashMap<Job, String>();

		for (final Job job : jobs) {
			final long now = System.currentTimeMillis();
			job.getConfiguration().setLong(JOB_SUBMISSION_TIME_KEY, now);
			job.submit();
			runningJobs.add(job);
		}

		while (!runningJobs.isEmpty()) {

			final Iterator<Job> it = runningJobs.iterator();
			while (it.hasNext()) {

				final Job job = it.next();
				if (job.isComplete()) {
					it.remove();
					final Configuration conf = job.getConfiguration();
					final long now = System.currentTimeMillis();
					final long timeFromSubmission = now - conf.getLong(JOB_SUBMISSION_TIME_KEY, -1L);
					System.out.println(job.getJobName() + " finished after " + timeFromSubmission);

					progressMap.remove(job);
					continue;
				}

				final String progressBar = getProgressBar(job.mapProgress(), job.reduceProgress());

				if (!progressBar.equals(progressMap.put(job, progressBar))) {
					System.out.println(job.getJobName() + " " + progressBar);
				}
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
