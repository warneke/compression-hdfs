package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import edu.berkeley.icsi.cdfs.cache.EvictionEntry;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface DataNodeNameNodeProtocol extends CommonNameNodeProtocol {

	void createNewBlock(PathWrapper cdfsPath, PathWrapper hdfsPath, int blockIndex, int blockLength) throws IOException;

	void reportCachedBlock(PathWrapper cdfsPath, int blockIndex, boolean compressed, String host) throws IOException;

	EvictionEntry getFileToEvict(String host) throws IOException;

	void confirmEviction(PathWrapper cdfsPath, int blockIndex, boolean compressed, String host) throws IOException;

	void registerDataNode(String hostname, int port) throws IOException;
}
