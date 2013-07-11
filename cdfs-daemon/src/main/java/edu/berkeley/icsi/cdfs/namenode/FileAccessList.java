package edu.berkeley.icsi.cdfs.namenode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.PopularBlock;
import edu.berkeley.icsi.cdfs.PopularFile;

final class FileAccessList {

	private static final Log LOG = LogFactory.getLog(FileAccessList.class);

	private static final String PERSISTENT_FILENAME = "pt.dat";

	private static final int REPORT_INTERVAL = 1000;

	private static final int HOT_SET_SIZE = 10;

	private static final class FileAccessListEntry {

		private final FileMetaData file;

		private int index;

		private double accessCount = 0.0;

		private FileAccessListEntry prev = null;

		private FileAccessListEntry next = null;

		private FileAccessListEntry(final FileMetaData file, final int index) {
			this.file = file;
			this.index = index;
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

	private PopularFile[] cachedPopularFiles = null;

	private FileAccessListEntry head = null;

	private FileAccessListEntry tail = null;

	private long counter = 0;

	private int nextElementIndex = 1;

	FileAccessList(final MetaDataStore mds) {
		try {
			loadPopularityList(mds);
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

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
			sb.append(current.index);
			sb.append('\t');
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
			entry = new FileAccessListEntry(fmd, this.nextElementIndex++);
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
		}

		// Increase access count
		entry.accessCount += getIncreaseCountValue(fmd);

		while (true) {

			final FileAccessListEntry prev = entry.prev;

			// We need to rebuild the hot set
			if (entry.index <= HOT_SET_SIZE) {
				this.cachedPopularFiles = null;
			}

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

			final int oldIndex = entry.index;
			entry.index = prev.index;
			prev.index = oldIndex;

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

		if (++this.counter % REPORT_INTERVAL == 0L) {

			printAccessCounts();
		}
	}

	PopularFile[] getPopularFiles(final int maximumNumberOfFiles) {

		if (this.cachedPopularFiles != null) {
			return this.cachedPopularFiles;
		}

		final List<PopularFile> popularFiles = new ArrayList<PopularFile>(maximumNumberOfFiles);

		FileAccessListEntry entry = this.head;
		for (int i = 0; i < maximumNumberOfFiles; ++i) {

			if (entry == null) {
				break;
			}

			final int numberOfBlocks = entry.file.getNumberOfBlocks();
			final PopularBlock[] popularBlocks = new PopularBlock[numberOfBlocks];
			final Iterator<BlockMetaData> it = entry.file.getBlockIterator();
			for (int j = 0; j < numberOfBlocks; ++j) {

				final BlockMetaData bmd = it.next();
				popularBlocks[j] = new PopularBlock(bmd.getIndex(), bmd.getUncompressedLength(),
					bmd.getCompressedLength());
			}

			final double popularityFactor = (entry.accessCount * (double) numberOfBlocks) / (double) this.counter;

			popularFiles.add(new PopularFile(entry.file.getPath(), popularBlocks, popularityFactor));

			entry = entry.next;
		}

		this.cachedPopularFiles = popularFiles.toArray(new PopularFile[0]);

		return this.cachedPopularFiles;
	}

	void shutDown() {

		try {
			savePopularityList();
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	private void savePopularityList() throws IOException {

		final FileOutputStream fos = new FileOutputStream(PERSISTENT_FILENAME);
		final DataOutputStream dos = new DataOutputStream(fos);

		try {

			dos.writeLong(this.counter);
			dos.writeInt(this.nextElementIndex);

			FileAccessListEntry entry = this.head;
			int c = 0;
			while (entry != null) {

				++c;

				dos.writeUTF(entry.file.getPath().toUri().getPath());
				dos.writeInt(entry.index);
				dos.writeDouble(entry.accessCount);

				entry = entry.next;
			}

			LOG.info("Writing " + c + " elements, " + this.nextElementIndex);

		} finally {
			dos.close();
		}
	}

	private void loadPopularityList(final MetaDataStore mds) throws IOException {

		final File persistentFile = new File(PERSISTENT_FILENAME);
		if (!persistentFile.exists()) {
			return;
		}

		final FileInputStream fis = new FileInputStream(persistentFile);
		final DataInputStream dis = new DataInputStream(fis);

		try {

			this.counter = dis.readLong();
			this.nextElementIndex = dis.readInt();

			final int numberOfFiles = this.nextElementIndex - 1;

			FileAccessListEntry previousEntry = null;
			LOG.info("Expecting to read " + numberOfFiles + " files");
			for (int i = 0; i < numberOfFiles; ++i) {

				final String path = dis.readUTF();
				final FileMetaData fmd = mds.getMetaDataByPath(path);
				if (fmd == null) {
					throw new IllegalStateException("Cannot find metadata to file " + path);
				}
				final int index = dis.readInt();
				final double accessCount = dis.readDouble();

				final FileAccessListEntry entry = new FileAccessListEntry(fmd, index);
				entry.accessCount = accessCount;

				if (previousEntry == null) {
					this.head = entry;
				} else {
					previousEntry.next = entry;
				}

				entry.prev = previousEntry;
				this.tail = entry;
				previousEntry = entry;

				this.lookup.put(fmd, entry);

				LOG.info("Read " + path + ", " + index + ", " + accessCount);
			}

		} finally {
			dis.close();
		}
	}
}
