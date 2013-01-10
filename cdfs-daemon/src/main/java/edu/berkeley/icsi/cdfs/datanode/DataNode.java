package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.ConfigUtils;

public class DataNode {

	private static final Log LOG = LogFactory.getLog(DataNode.class);

	private final ServerSocket serverSocket;

	private final DataNodeNameNodeProtocol nameNode;

	private final Configuration conf;

	public DataNode(final Configuration conf) throws IOException {
		this.serverSocket = new ServerSocket(CDFS.DATANODE_DATA_PORT);
		this.conf = conf;

		this.nameNode = (DataNodeNameNodeProtocol) RPC.getProxy(
			DataNodeNameNodeProtocol.class, 1, new InetSocketAddress(
				"localhost", CDFS.NAMENODE_RPC_PORT), this.conf);
	}

	void run() throws IOException {

		while (true) {
			final Socket socket = this.serverSocket.accept();
			new Connection(socket, this.nameNode, this.conf);
		}
	}

	public static void main(final String[] args) {

		// Load the configuration
		final Configuration conf;
		try {
			conf = ConfigUtils.loadConfiguration(args);
		} catch (ConfigurationException e) {
			LOG.error(e.getMessage());
			return;
		}

		DataNode dataNode = null;

		try {

			dataNode = new DataNode(conf);
			dataNode.run();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (dataNode != null) {
				dataNode.shutDown();
			}
		}
	}

	void shutDown() {

		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
