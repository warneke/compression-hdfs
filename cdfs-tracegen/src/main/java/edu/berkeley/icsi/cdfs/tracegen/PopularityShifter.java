package edu.berkeley.icsi.cdfs.tracegen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

final class PopularityShifter {

	private static final class AscendingComparator implements Comparator<File> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compare(final File o1, final File o2) {

			final long diff = o1.getCompressedFileSize() - o2.getCompressedFileSize();

			if (diff > Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			} else if (diff < Integer.MIN_VALUE) {
				return Integer.MIN_VALUE;
			}

			return (int) diff;
		}

	}

	private PopularityShifter() {
	}

	private static long computeNumberOfBytesStored(final List<File> files) {

		long totalNumberOfBytes = 0L;
		final Iterator<File> it = files.iterator();
		while (it.hasNext()) {
			totalNumberOfBytes += it.next().getUncompressedFileSize();
		}

		return totalNumberOfBytes;
	}

	static void adjust(final List<File> files, final FilePopularityDistribution fpd) {

		final long numberOfBytesStored = computeNumberOfBytesStored(files);

		// Sort input files by size
		Collections.sort(files, new AscendingComparator());

		while (true) {

			final List<File> seq = new ArrayList<File>();
			final Set<File> tmpSet = new HashSet<File>(files.size());
			tmpSet.addAll(files);

			while (!tmpSet.isEmpty()) {
				final File file = files.get(fpd.sample() - 1);
				tmpSet.remove(file);

				seq.add(file);
			}

			Collections.sort(seq, new AscendingComparator());

			final int nintyPercent = (int) Math.floor((double) seq.size() * 0.9);
			long numberOfBytesAccessed = 0L;

			for (int i = 0; i < nintyPercent; ++i) {
				final File file = seq.get(i);
				if (tmpSet.add(file)) {
					numberOfBytesAccessed += file.getUncompressedFileSize();
				}
			}

			if (((double) numberOfBytesAccessed / (double) numberOfBytesStored) < 0.16) {
				shift(files);
			} else {
				break;
			}
		}
	}

	private static void shift(final List<File> files) {

		final File file = files.remove(0);
		files.add(file);
	}
}
