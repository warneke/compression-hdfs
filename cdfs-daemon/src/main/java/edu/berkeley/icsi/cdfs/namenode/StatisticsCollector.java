package edu.berkeley.icsi.cdfs.namenode;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.statistics.AbstractStatisticsParser;
import edu.berkeley.icsi.cdfs.statistics.AbstractUserStatistics;
import edu.berkeley.icsi.cdfs.statistics.ReadStatistics;

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

		final boolean prefetchBlocks = conf.getBoolean(ConfigConstants.ENABLE_BLOCK_PREFETCHING_KEY,
			ConfigConstants.DEFAULT_ENABLE_BLOCK_PREFETCHING);

		final StringBuilder sb = new StringBuilder("statistics-");
		sb.append(cacheUncompressed ? 'U' : 'u');
		sb.append(cacheCompressed ? 'C' : 'c');
		sb.append(prefetchBlocks ? 'P' : 'p');
		sb.append('-');
		sb.append(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

		return sb.toString();
	}

	void shutDown() {

		try {
			synchronized (this.outputStream) {
				this.outputStream.close();
			}
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}
	}

	void collectUserStatistics(final AbstractUserStatistics userStatistics) throws IOException {

		synchronized (this.outputStream) {
			AbstractStatisticsParser.toOutputStream(userStatistics, this.outputStream);
		}
	}

	void collectReadStatistics(final List<ReadStatistics> readStatistics) throws IOException {

		synchronized (this.outputStream) {
			final Iterator<ReadStatistics> it = readStatistics.iterator();
			while (it.hasNext()) {
				AbstractStatisticsParser.toOutputStream(it.next(), this.outputStream);
			}
		}
	}
}
