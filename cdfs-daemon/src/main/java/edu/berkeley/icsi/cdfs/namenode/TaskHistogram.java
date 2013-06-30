package edu.berkeley.icsi.cdfs.namenode;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.berkeley.icsi.cdfs.utils.HostUtils;

final class TaskHistogram {

	private static final Log LOG = LogFactory.getLog(TaskHistogram.class);

	private static final int REPORT_INTERVAL = 500;

	private static final ConcurrentMap<String, AtomicInteger> HISTOGRAM = new ConcurrentSkipListMap<String, AtomicInteger>();

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private TaskHistogram() {
	}

	static void registerHost(final String host) {

		HISTOGRAM.putIfAbsent(HostUtils.stripFQDN(host), new AtomicInteger(0));
	}

	static void increaseHostCount(final String host) {

		final String strippedHost = HostUtils.stripFQDN(host);
		final AtomicInteger taskCount = HISTOGRAM.get(strippedHost);

		if (taskCount == null) {
			LOG.error("No entry for host " + strippedHost);
			return;
		}

		taskCount.incrementAndGet();

		if (COUNTER.incrementAndGet() == REPORT_INTERVAL) {
			COUNTER.set(0);

			final Iterator<Map.Entry<String, AtomicInteger>> it = HISTOGRAM.entrySet().iterator();

			final StringBuilder sb = new StringBuilder();

			while (it.hasNext()) {

				final Map.Entry<String, AtomicInteger> entry = it.next();

				sb.append(entry.getKey());
				sb.append(":\t");
				sb.append(entry.getValue().intValue());
				sb.append('\n');
			}

			LOG.info(sb.toString());
		}
	}
}
