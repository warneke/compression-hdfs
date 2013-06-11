package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.statistics.UserStatistics;

final class StatisticsCollector {

	public static final String JOB_NAME_CONF_KEY = "job.statistics.name";

	private final String jobID;

	private final boolean isMap;

	private final int taskID;

	private final long startTime;

	private final CDFS cdfs;

	StatisticsCollector(final Configuration conf, final boolean isMap, final int taskID) throws IOException {

		this.jobID = conf.get(JOB_NAME_CONF_KEY);
		if (this.jobID == null) {
			throw new IllegalStateException("Cannot determine job name");
		}

		this.isMap = isMap;
		this.taskID = taskID;
		this.startTime = System.currentTimeMillis();

		final Path cdfsPath = new Path(conf.get(ConfigConstants.CDFS_DEFAULT_NAME_KEY,
			ConfigConstants.DEFEAULT_CDFS_DEFAULT_NAME));

		this.cdfs = (CDFS) cdfsPath.getFileSystem(conf);
	}

	void close() throws IOException {

		final UserStatistics us = new UserStatistics(this.jobID, this.isMap, this.taskID, this.startTime,
			System.currentTimeMillis());

		this.cdfs.reportUserStatistics(us);
	}
}
