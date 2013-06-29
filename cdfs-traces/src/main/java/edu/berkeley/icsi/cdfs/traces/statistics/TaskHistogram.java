package edu.berkeley.icsi.cdfs.traces.statistics;

import java.util.Collection;
import java.util.Iterator;

import edu.berkeley.icsi.cdfs.traces.TraceJob;

public final class TaskHistogram extends AbstractHistogram<Integer> {

	public TaskHistogram(final Collection<TraceJob> jobs) {

		final Iterator<TraceJob> it = jobs.iterator();
		while (it.hasNext()) {

			final TraceJob tj = it.next();
			final Integer numberOfTasks = Integer.valueOf(tj.getNumberOfMapTasks() + tj.getNumberOfReduceTasks());

			add(numberOfTasks);
		}
	}
}
