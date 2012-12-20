package edu.berkeley.icsi.cdfs.namenode;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.protocols.ClientNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public class NameNode implements ClientNameNodeProtocol {

	private final Server rpcServer;

	private final ConcurrentHashMap<Path, Object> metaData;

	NameNode() throws IOException {

		this.rpcServer = RPC.getServer(this, "localhost", CDFS.NAMENODE_RPC_PORT, new Configuration());
		this.rpcServer.start();

		this.metaData = new ConcurrentHashMap<Path, Object>();
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

		if (!this.metaData.containsKey(path.getPath())) {
			return null;
		}

		System.out.println("Path " + path.getPath());

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean create(final PathWrapper path) throws IOException {

		if (this.metaData.containsKey(path.getPath())) {
			return false;
		}

		return true;
	}
}
