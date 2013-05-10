package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import edu.berkeley.icsi.cdfs.cache.EvictionList;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface DataNodeNameNodeProtocol extends CommonNameNodeProtocol {

	void createNewBlock(PathWrapper cdfsPath, PathWrapper hdfsPath, int blockIndex, int blockLength) throws IOException;

	void reportUncompressedCachedBlock(PathWrapper cdfsPath, int blockIndex, String host) throws IOException;

	void reportCompressedCachedBlock(PathWrapper cdfsPath, int blockIndex, String host) throws IOException;

	EvictionList getFilesToEvict(String host) throws IOException;
}
