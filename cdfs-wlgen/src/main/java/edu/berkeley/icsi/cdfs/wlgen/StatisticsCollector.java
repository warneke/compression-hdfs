package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.statistics.AbstractUserStatistics;
import edu.berkeley.icsi.cdfs.statistics.MapUserStatistics;
import edu.berkeley.icsi.cdfs.statistics.ReduceUserStatistics;

final class StatisticsCollector {

	public static final String JOB_NAME_CONF_KEY = "job.statistics.name";

	private final String jobID;

	private final int taskID;

	private final boolean isMap;

	private final Path path;

	private final int blockIndex;

	private final long startTime;

	private final CDFS cdfs;

	private StatisticsCollector(final Configuration conf, final int taskID, final boolean isMap, final Path path,
			final int blockIndex) throws IOException {

		this.jobID = conf.get(JOB_NAME_CONF_KEY);
		if (this.jobID == null) {
			throw new IllegalStateException("Cannot determine job name");
		}

		this.taskID = taskID;
		this.isMap = isMap;

		this.path = path;
		this.blockIndex = blockIndex;
		this.startTime = System.currentTimeMillis();

		final Path cdfsPath = new Path(conf.get(ConfigConstants.CDFS_DEFAULT_NAME_KEY,
			ConfigConstants.DEFEAULT_CDFS_DEFAULT_NAME));

		this.cdfs = (CDFS) cdfsPath.getFileSystem(conf);
	}

	static StatisticsCollector forMap(final Configuration conf, final int taskID, final Path path, final int blockIndex)
			throws IOException {

		return new StatisticsCollector(conf, taskID, true, path, blockIndex);
	}

	static StatisticsCollector forReduce(final Configuration conf, final int taskID) throws IOException {

		return new StatisticsCollector(conf, taskID, false, null, -1);
	}

	void close() throws IOException {

		final AbstractUserStatistics us;
		if (this.isMap) {
			us = new MapUserStatistics(this.jobID, this.taskID, this.startTime, System.currentTimeMillis(), this.path,
				this.blockIndex);
		} else {
			us = new ReduceUserStatistics(this.jobID, this.taskID, this.startTime, System.currentTimeMillis());
		}

		this.cdfs.reportUserStatistics(us);
	}
}
