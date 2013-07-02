package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.VersionedProtocol;

import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.ConnectionInfo;
import edu.berkeley.icsi.cdfs.statistics.AbstractUserStatistics;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface ClientNameNodeProtocol extends VersionedProtocol {

	boolean create(PathWrapper path, boolean overwrite) throws IOException;

	boolean mkdirs(PathWrapper path, FsPermission permission) throws IOException;

	FileStatus getFileStatus(PathWrapper path) throws IOException;

	ConnectionInfo determineClosestDataNode(String hostname) throws IOException;

	void reportUserStatistics(AbstractUserStatistics userStatistics) throws IOException;
	
	CDFSBlockLocation[] getFileBlockLocations(PathWrapper path, long start, long len) throws IOException;
}
