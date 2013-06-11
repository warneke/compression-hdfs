package edu.berkeley.icsi.cdfs.namenode;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.statistics.AbstractStatisticsParser;
import edu.berkeley.icsi.cdfs.statistics.UserStatistics;

final class StatisticsCollector {

	private static final Log LOG = LogFactory.getLog(StatisticsCollector.class);

	private final DataOutputStream outputStream;

	StatisticsCollector(final Configuration conf) throws IOException {

		final String filename = getFilename(conf);

		this.outputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));

		LOG.info("Writing statistics to " + filename);
	}

	private static String getFilename(final Configuration conf) {

		final boolean cacheCompressed = conf.getBoolean(ConfigConstants.ENABLE_COMPRESSED_CACHING_KEY,
			ConfigConstants.DEFAULT_ENABLE_COMPRESSED_CACHING);

		final boolean cacheUncompressed = conf.getBoolean(ConfigConstants.ENABLE_UNCOMPRESSED_CACHING_KEY,
			ConfigConstants.DEFAULT_ENABLE_UNCOMPRESSED_CACHING);

		final StringBuilder sb = new StringBuilder("statistics-");
		sb.append(cacheUncompressed ? 'U' : 'u');
		sb.append(cacheCompressed ? 'C' : 'c');

		return sb.toString();
	}

	void stop() {

		try {
			synchronized (this.outputStream) {
				this.outputStream.close();
			}
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}
	}

	void collectUserStatistics(final UserStatistics userStatistics) throws IOException {

		synchronized (this.outputStream) {
			AbstractStatisticsParser.toOutputStream(userStatistics, this.outputStream);
		}
	}
}
