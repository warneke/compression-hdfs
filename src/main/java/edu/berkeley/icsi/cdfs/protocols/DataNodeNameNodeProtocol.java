package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface DataNodeNameNodeProtocol extends VersionedProtocol {

	void createNewBlock(PathWrapper cdfsPath, PathWrapper hdfsPath, int blockIndex, int blockLength) throws IOException;
}
