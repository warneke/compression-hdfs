package edu.berkeley.icsi.cdfs.protocols;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public interface ClientNameNodeProtocol extends CommonNameNodeProtocol {

	boolean create(PathWrapper path, boolean overwrite) throws IOException;

	boolean mkdirs(PathWrapper path, FsPermission permission) throws IOException;

	FileStatus getFileStatus(PathWrapper path) throws IOException;
}
