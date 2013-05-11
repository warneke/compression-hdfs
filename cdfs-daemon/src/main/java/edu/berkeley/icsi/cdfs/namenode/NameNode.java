package edu.berkeley.icsi.cdfs.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.cache.EvictionList;
import edu.berkeley.icsi.cdfs.protocols.ClientNameNodeProtocol;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public class NameNode implements ClientNameNodeProtocol, DataNodeNameNodeProtocol {

	private final Server rpcServer;

	private final MetaDataStore metaDataStore;

	private final FileSystem hdfs;

	NameNode() throws IOException {

		final Configuration conf = new Configuration();

		this.rpcServer = RPC.getServer(this, "localhost", CDFS.NAMENODE_RPC_PORT, conf);
		this.rpcServer.start();

		this.hdfs = new Path("hdfs://localhost:" + CDFS.HDFS_NAMENODE_PORT).getFileSystem(conf);

		this.metaDataStore = new MetaDataStore(hdfs);
	}

	public void shutDown() {
		this.rpcServer.stop();
	}

	public static void main(final String[] args) {

		NameNode nameNode = null;

		try {

			nameNode = new NameNode();
			while (true) {
				try {
					Thread.sleep(5000L);
				} catch (InterruptedException ie) {
					break;
				}
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (nameNode != null) {
				nameNode.shutDown();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {

		return 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(final PathWrapper path) throws IOException {

		return this.metaDataStore.getFileStatus(path.getPath());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean create(final PathWrapper path, final boolean overwrite) throws IOException {

		return this.metaDataStore.create(path.getPath(), overwrite);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createNewBlock(final PathWrapper cdfsPath, final PathWrapper hdfsPath, final int blockIndex,
			final int blockLength) throws IOException {

		this.metaDataStore.addNewBlock(cdfsPath.getPath(), hdfsPath.getPath(), blockIndex, blockLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean mkdirs(final PathWrapper path, final FsPermission permission) throws IOException {

		// We don't care about the directory structure, so let HDFS handle this
		synchronized (this.hdfs) {
			return this.hdfs.mkdirs(CDFS.toHDFSPath(path.getPath(), ""), permission);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CDFSBlockLocation[] getFileBlockLocations(final PathWrapper path, final long start, final long len)
			throws IOException {

		return this.metaDataStore.getFileBlockLocations(path.getPath(), start, len);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportUncompressedCachedBlock(final PathWrapper cdfsPath, final int blockIndex, final String host)
			throws IOException {

		this.metaDataStore.reportUncompressedCachedBlock(cdfsPath.getPath(), blockIndex, host);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportCompressedCachedBlock(final PathWrapper cdfsPath, final int blockIndex, final String host)
			throws IOException {

		this.metaDataStore.reportCompressedCachedBlock(cdfsPath.getPath(), blockIndex, host);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public EvictionList getFilesToEvict(final String host) throws IOException {

		return this.metaDataStore.getFilesToEvictLIFE(host);
	}
}
