package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public final class StatisticsAnalysis extends AbstractStatisticsParser {

	private final Map<String, MapReduceJob> mapReduceJobs = new TreeMap<String, MapReduceJob>();

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

		sa.showStatistics();
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
	public void processUserStatistics(final UserStatistics userStatistics) {

		final String jobID = userStatistics.getJobID();
		MapReduceJob mrj = this.mapReduceJobs.get(jobID);
		if (mrj == null) {
			mrj = new MapReduceJob(jobID);
			this.mapReduceJobs.put(jobID, mrj);
		}

		if (userStatistics.isMap()) {
			mrj.addMapTask(userStatistics.getTaskID(), userStatistics.getStartTime(), userStatistics.getEndTime());
		} else {
			mrj.addReduceTask(userStatistics.getTaskID(), userStatistics.getStartTime(), userStatistics.getEndTime());
		}
	}
}
