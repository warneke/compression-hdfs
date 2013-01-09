package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface CommonNameNodeProtocol extends VersionedProtocol {

	CDFSBlockLocation[] getFileBlockLocations(PathWrapper path, long start, long len) throws IOException;
}
