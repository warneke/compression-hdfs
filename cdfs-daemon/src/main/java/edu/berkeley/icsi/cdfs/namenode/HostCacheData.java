package edu.berkeley.icsi.cdfs.namenode;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import edu.berkeley.icsi.cdfs.cache.EvictionEntry;

class HostCacheData {

	private final Map<FileMetaData, Set<BlockMetaData>> cachedCompressedBlocks = new TreeMap<FileMetaData, Set<BlockMetaData>>();

	private final Map<FileMetaData, Set<BlockMetaData>> cachedUncompressedBlocks = new TreeMap<FileMetaData, Set<BlockMetaData>>();

	EvictionEntry getLargestCompressedIncompleteFile() {
		return getLargestFile(true, false);
	}

	EvictionEntry getLargestUncompressedIncompleteFile() {
		return getLargestFile(false, false);
	}

	EvictionEntry getLargestCompressedCompleteFile() {
		return getLargestFile(true, true);
	}

	EvictionEntry getLargestUncompressedCompleteFile() {
		return getLargestFile(false, true);
	}

	EvictionEntry getLeastAccessedCompressedIncompleteFile() {
		return getLeastAccessedFile(true, false);
	}

	EvictionEntry getLeastAccessedUncompressedIncompleteFile() {
		return getLeastAccessedFile(false, false);
	}

	EvictionEntry getLeastAccessedCompressedCompleteFile() {
		return getLeastAccessedFile(true, true);
	}

	EvictionEntry getLeastAccessedUncompressedCompleteFile() {
		return getLeastAccessedFile(false, true);
	}

	private EvictionEntry getLeastAccessedFile(final boolean compressed, final boolean complete) {

		final Map<FileMetaData, Set<BlockMetaData>> cachedBlocks;
		if (compressed) {
			cachedBlocks = this.cachedCompressedBlocks;
		} else {
			cachedBlocks = this.cachedUncompressedBlocks;
		}

		FileMetaData leastAccessedFile = null;
		final Iterator<FileMetaData> it = cachedBlocks.keySet().iterator();
		while (it.hasNext()) {

			final FileMetaData fmd = it.next();
			if (complete != fmd.isCachedCompletely(compressed)) {
				continue;
			}

			if (leastAccessedFile == null) {
				leastAccessedFile = fmd;
			} else {
				if (fmd.getAccessCount() < leastAccessedFile.getAccessCount()) {
					leastAccessedFile = fmd;
				}
			}
		}

		if (leastAccessedFile == null) {
			return null;
		}

		return new EvictionEntry(leastAccessedFile.getPath(), leastAccessedFile.getNumberOfBlocks(), compressed);
	}

	private EvictionEntry getLargestFile(final boolean compressed, final boolean complete) {

		final Map<FileMetaData, Set<BlockMetaData>> cachedBlocks;
		if (compressed) {
			cachedBlocks = this.cachedCompressedBlocks;
		} else {
			cachedBlocks = this.cachedUncompressedBlocks;
		}

		final Iterator<FileMetaData> it = cachedBlocks.keySet().iterator();
		while (it.hasNext()) {
			final FileMetaData fmd = it.next();
			if (complete == fmd.isCachedCompletely(compressed)) {
				return new EvictionEntry(fmd.getPath(), fmd.getNumberOfBlocks(), compressed);
			}
		}

		return null;
	}

	void add(final FileMetaData fmd, final BlockMetaData bmd, final boolean compressed) {

		final Map<FileMetaData, Set<BlockMetaData>> cachedBlocks;
		if (compressed) {
			cachedBlocks = this.cachedCompressedBlocks;
		} else {
			cachedBlocks = this.cachedUncompressedBlocks;
		}

		Set<BlockMetaData> blockSet = cachedBlocks.get(fmd);
		if (blockSet == null) {
			blockSet = new HashSet<BlockMetaData>();
			cachedBlocks.put(fmd, blockSet);
		}
		blockSet.add(bmd);
	}

	void remove(final FileMetaData fmd, final BlockMetaData bmd, final boolean compressed) {

		final Map<FileMetaData, Set<BlockMetaData>> cachedBlocks;
		if (compressed) {
			cachedBlocks = this.cachedCompressedBlocks;
		} else {
			cachedBlocks = this.cachedUncompressedBlocks;
		}

		final Set<BlockMetaData> blockSet = cachedBlocks.get(fmd);

		if (blockSet == null) {
			return;
		}

		blockSet.remove(bmd);

		if (blockSet.isEmpty()) {
			cachedBlocks.remove(fmd);
		}
	}
}
