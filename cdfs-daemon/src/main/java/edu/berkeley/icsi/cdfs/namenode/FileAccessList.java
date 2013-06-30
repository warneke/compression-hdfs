package edu.berkeley.icsi.cdfs.namenode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

final class FileAccessList {

	private static final Log LOG = LogFactory.getLog(FileAccessList.class);

	private static final int REPORT_INTERVAL = 1000;

	private static final class FileAccessListEntry {

		private final FileMetaData file;

		private double accessCount;

		private FileAccessListEntry prev = null;

		private FileAccessListEntry next = null;

		private FileAccessListEntry(final FileMetaData file, final double accessCount) {
			this.file = file;
			this.accessCount = accessCount;
		}
	}

	private static final class FileAccessListIterator implements Iterator<FileMetaData> {

		private FileAccessListEntry current;

		private final boolean forward;

		private FileAccessListIterator(final FileAccessListEntry current, final boolean forward) {

			this.current = current;
			this.forward = forward;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean hasNext() {

			return (this.current != null);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public FileMetaData next() {

			if (this.current == null) {
				throw new NoSuchElementException();
			}

			final FileMetaData fmd = this.current.file;

			if (this.forward) {
				this.current = this.current.next;
			} else {
				this.current = this.current.prev;
			}

			return fmd;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void remove() {

			throw new UnsupportedOperationException();
		}

	}

	private final Map<FileMetaData, FileAccessListEntry> lookup = new HashMap<FileMetaData, FileAccessListEntry>();

	private FileAccessListEntry head = null;

	private FileAccessListEntry tail = null;

	private int counter = 0;

	Iterator<FileMetaData> iterator() {

		return new FileAccessListIterator(this.head, true);
	}

	Iterator<FileMetaData> reverseIterator() {

		return new FileAccessListIterator(this.tail, false);
	}

	private void printAccessCounts() {

		final StringBuilder sb = new StringBuilder();
		FileAccessListEntry current = this.head;

		while (current != null) {

			sb.append(current.file.getPath());
			sb.append(":\t");
			sb.append(current.accessCount);
			if (current.next != null) {
				sb.append('\n');
			}

			current = current.next;
		}

		LOG.info(sb.toString());
	}

	private static double getIncreaseCountValue(final FileMetaData fmd) {

		return 1.0 / (double) fmd.getNumberOfBlocks();
	}

	void increaseAccessCount(final FileMetaData fmd) {

		FileAccessListEntry entry = this.lookup.get(fmd);
		if (entry == null) {
			entry = new FileAccessListEntry(fmd, getIncreaseCountValue(fmd));
			this.lookup.put(fmd, entry);

			// Add entry to tail of list
			if (this.tail == null) {
				// List is empty
				this.tail = entry;
				this.head = entry;
			} else {

				final FileAccessListEntry oldTail = this.tail;
				this.tail = entry;
				entry.prev = oldTail;
				oldTail.next = entry;
			}

			// No need to adjust anything
			return;
		}

		// Increase access count
		entry.accessCount += getIncreaseCountValue(fmd);

		while (true) {

			final FileAccessListEntry prev = entry.prev;

			if (prev == null) {
				break;
			}

			if (prev.accessCount >= entry.accessCount) {
				break;
			}

			// Swap prev and entry
			final FileAccessListEntry next = entry.next;
			final FileAccessListEntry prevPrev = prev.prev;

			entry.prev = prevPrev;
			entry.next = prev;
			prev.prev = entry;
			prev.next = next;

			if (next != null) {
				next.prev = prev;
			}

			if (prevPrev != null) {
				prevPrev.next = entry;
			}

			if (entry == this.tail) {
				this.tail = prev;
			}

			if (prev == this.head) {
				this.head = entry;
			}
		}

		if (++this.counter == REPORT_INTERVAL) {
			this.counter = 0;

			printAccessCounts();
		}
	}
}
