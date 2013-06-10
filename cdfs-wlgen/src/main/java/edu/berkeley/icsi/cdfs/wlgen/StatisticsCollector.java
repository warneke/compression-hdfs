package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class StatisticsCollector {

	public static final String JOB_NAME_CONF_KEY = "job.statistics.name";

	private final FSDataOutputStream outputStream;

	StatisticsCollector(final Configuration conf, final int taskID, final boolean map) throws IOException {

		final Path path = getStatisticsPath(conf);

		final FileSystem fs = path.getFileSystem(conf);
		this.outputStream = fs.append(path);

		// Write prefix
		final byte[] buf = new byte[5];
		buf[0] = (byte) (map ? 'M' : 'R');
		NumberUtils.integerToByteArray(taskID, buf, 1);
		this.outputStream.write(buf);
		writeTimestamp();
	}

	void close() throws IOException {
		writeTimestamp();
		this.outputStream.close();
	}

	private void writeTimestamp() throws IOException {

		final byte[] buf = new byte[8];
		NumberUtils.longToByteArray(System.currentTimeMillis(), buf, 0);
		this.outputStream.write(buf);
	}

	private static Path getStatisticsPath(final Configuration conf) {

		final String jobName = conf.get(JOB_NAME_CONF_KEY);
		if (jobName == null) {
			throw new IllegalStateException("Cannot determine job name");
		}

		final String hdfsBase = conf.get(ConfigConstants.CDFS_DEFAULT_NAME_KEY,
			ConfigConstants.DEFEAULT_CDFS_DEFAULT_NAME);

		final StringBuilder sb = new StringBuilder(hdfsBase);
		sb.append(Path.SEPARATOR_CHAR);
		sb.append(jobName);

		return new Path(sb.toString());
	}
}
