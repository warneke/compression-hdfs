package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface DataNodeNameNodeProtocol extends CommonNameNodeProtocol {

	void createNewBlock(PathWrapper cdfsPath, PathWrapper hdfsPath, int blockIndex, int blockLength) throws IOException;
}
