package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;

public final class StatisticsAnalysis extends AbstractStatisticsParser {

	private final Map<String, MapReduceJob> mapReduceJobs = new TreeMap<String, MapReduceJob>();

	private final Map<BlockKey, List<ReadStatistics>> readStatistics = new HashMap<BlockKey, List<ReadStatistics>>();

	private static class BlockKey {

		private final Path path;

		private final int index;

		private BlockKey(final Path path, final int index) {
			this.path = path;
			this.index = index;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(final Object obj) {

			if (!(obj instanceof BlockKey)) {
				return false;
			}

			final BlockKey bk = (BlockKey) obj;

			if (!this.path.equals(bk.path)) {
				return false;
			}

			if (this.index != bk.index) {
				return false;
			}

			return true;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {

			return this.path.hashCode();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String toString() {

			return this.path + " [" + this.index + "]";
		}
	}

	public static void main(final String[] args) {

		if (args.length < 1) {
			System.err.println("No statistics file specified");
			return;
		}

		final StatisticsAnalysis sa = new StatisticsAnalysis();

		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new FileInputStream(args[0]));
			sa.parse(dis);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (dis != null) {
				try {
					dis.close();
				} catch (IOException e) {
				}
			}
		}

		// Map the read statistics
		sa.mapReadStatistics();

		// Print out the statistics
		sa.showStatistics();
	}

	private void mapReadStatistics() {

		final Iterator<MapReduceJob> it = this.mapReduceJobs.values().iterator();
		while (it.hasNext()) {

			final MapReduceJob mrj = it.next();
			final Iterator<MapTask> it2 = mrj.iterator();

			while (it2.hasNext()) {

				final MapTask mt = it2.next();
				final BlockKey key = new BlockKey(mrj.getInputFile(), mt.getBlockIndex());
				final List<ReadStatistics> list = this.readStatistics.get(key);
				if (list == null) {
					System.err.println("No read statistics for " + key);
					continue;
				}

				ReadStatistics minRs = null;
				long minDiff = Long.MAX_VALUE;

				final Iterator<ReadStatistics> it3 = list.iterator();
				while (it3.hasNext()) {

					final ReadStatistics rs = it3.next();
					final long diff = Math.abs(rs.getTime() - mt.getStartTime());
					if (diff < minDiff) {
						minDiff = diff;
						minRs = rs;
					}
				}

				list.remove(minDiff);

				if (!minRs.isDiskRead()) {
					mt.markCached();
				}
			}
		}
	}

	private void showStatistics() {

		final Iterator<MapReduceJob> it = this.mapReduceJobs.values().iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processMapUserStatistics(final MapUserStatistics userStatistics) {

		final String jobID = userStatistics.getJobID();
		MapReduceJob mrj = this.mapReduceJobs.get(jobID);
		if (mrj == null) {
			mrj = new MapReduceJob(jobID, userStatistics.getInputFile());
			this.mapReduceJobs.put(jobID, mrj);
		}

		mrj.addMapTask(userStatistics.getTaskID(), userStatistics.getStartTime(), userStatistics.getEndTime(),
			userStatistics.getBlockIndex());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processReduceUserStatistics(final ReduceUserStatistics userStatistics) {

		MapReduceJob mrj = this.mapReduceJobs.get(userStatistics.getJobID());
		if (mrj == null) {
			throw new IllegalStateException("Cannot find MapReduce job with ID " + userStatistics.getJobID());
		}

		mrj.addReduceTask(userStatistics.getTaskID(), userStatistics.getStartTime(), userStatistics.getEndTime());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processReadStatistics(final ReadStatistics readStatistics) {

		final BlockKey key = new BlockKey(readStatistics.getPath(), readStatistics.getIndex());

		List<ReadStatistics> list = this.readStatistics.get(key);
		if (list == null) {
			list = new ArrayList<ReadStatistics>();
			this.readStatistics.put(key, list);
		}
		list.add(readStatistics);
	}

}
