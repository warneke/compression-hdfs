package edu.berkeley.icsi.cdfs.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

final class BlockPrefetcher extends Thread {

	private static final Log LOG = LogFactory.getLog(BlockPrefetcher.class);

	private final ConnectionDispatcher connectionDispatcher;

	BlockPrefetcher(final ConnectionDispatcher connectionDispatcher) {
		super("Prefetcher thread");

		this.connectionDispatcher = connectionDispatcher;

		start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

	}

	void shutDown() {

		try {
			join();
		} catch (InterruptedException e) {
			LOG.warn(StringUtils.stringifyException(e));
		}
	}
}
