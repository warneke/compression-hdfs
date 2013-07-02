package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

import edu.berkeley.icsi.cdfs.BlockReadInformation;
import edu.berkeley.icsi.cdfs.PopularFile;
import edu.berkeley.icsi.cdfs.cache.EvictionEntry;
import edu.berkeley.icsi.cdfs.statistics.ReadStatistics;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;
import edu.berkeley.icsi.cdfs.utils.WritableArrayList;

public interface DataNodeNameNodeProtocol extends VersionedProtocol {

	void createNewBlock(PathWrapper cdfsPath, PathWrapper hdfsPath, int blockIndex, int blockLength) throws IOException;

	void reportCachedBlock(PathWrapper cdfsPath, int blockIndex, boolean compressed, String host) throws IOException;

	EvictionEntry getFileToEvict(String host) throws IOException;

	void confirmEviction(PathWrapper cdfsPath, int blockIndex, boolean compressed, String host) throws IOException;

	void registerDataNode(String hostname, int port) throws IOException;

	void reportReadStatistics(WritableArrayList<ReadStatistics> readStatistics, String host) throws IOException;

	BlockReadInformation[] getBlockReadInformation(PathWrapper path, long start, long len) throws IOException;

	PopularFile[] getPopularFiles(int maximumNumberOfFiles) throws IOException;
}
