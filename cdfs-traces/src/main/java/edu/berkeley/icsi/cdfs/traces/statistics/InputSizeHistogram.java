package edu.berkeley.icsi.cdfs.traces.statistics;

import java.util.Collection;
import java.util.Iterator;

import edu.berkeley.icsi.cdfs.traces.TraceJob;

public final class InputSizeHistogram extends AbstractHistogram<Long> {

	public InputSizeHistogram(final Collection<TraceJob> jobs) {

		final Iterator<TraceJob> it = jobs.iterator();
		while (it.hasNext()) {

			final TraceJob tj = it.next();
			final Long inputSize = Long.valueOf(tj.getInputFile().getUncompressedFileSize());

			add(inputSize);
		}
	}
}
