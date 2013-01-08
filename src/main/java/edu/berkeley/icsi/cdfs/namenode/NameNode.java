package edu.berkeley.icsi.cdfs.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.protocols.ClientNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public class NameNode implements ClientNameNodeProtocol {

	private final Server rpcServer;

	private final MetaDataStore metaDataStore;

	NameNode() throws IOException {

		this.rpcServer = RPC.getServer(this, "localhost", CDFS.NAMENODE_RPC_PORT, new Configuration());
		this.rpcServer.start();

		this.metaDataStore = new MetaDataStore();
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
	public boolean create(final PathWrapper path) throws IOException {

		return this.metaDataStore.create(path.getPath());
	}
}
