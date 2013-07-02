package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.conf.ConfigUtils;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.HostUtils;
import edu.berkeley.icsi.cdfs.utils.PathConverter;

public class DataNode implements ConnectionDispatcher {

	private static final Log LOG = LogFactory.getLog(DataNode.class);

	private final Set<Connection> activeConnectons;

	private final ServerSocket serverSocket;

	private final PathConverter pathConverter;

	private final DataNodeNameNodeProtocol nameNode;

	private final Configuration conf;

	private final String host;

	private final FileSystem hdfs;

	private final BlockPrefetcher blockPrefetcher;

	public DataNode(final Configuration conf) throws IOException {

		LOG.info("Starting CDFS datanode on port " + CDFS.DATANODE_DATA_PORT);

		this.activeConnectons = Collections.newSetFromMap(new ConcurrentHashMap<Connection, Boolean>());

		this.serverSocket = new ServerSocket(CDFS.DATANODE_DATA_PORT);
		this.conf = conf;

		// Read HDFS default path from configuration
		final String hdfsString = conf.get(ConfigConstants.HDFS_DEFAULT_NAME_KEY,
			ConfigConstants.DEFEAULT_HDFS_DEFAULT_NAME);
		final URI hdfsURI;
		try {
			hdfsURI = new URI(hdfsString);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

		// Read CDFS default path from configuration
		final String cdfsString = conf.get(ConfigConstants.CDFS_DEFAULT_NAME_KEY,
			ConfigConstants.DEFEAULT_CDFS_DEFAULT_NAME);
		URI cdfsURI;
		try {
			cdfsURI = new URI(cdfsString);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

		this.pathConverter = new PathConverter(hdfsURI);

		this.nameNode = (DataNodeNameNodeProtocol) RPC.getProxy(DataNodeNameNodeProtocol.class, 1,
			new InetSocketAddress(cdfsURI.getHost(), cdfsURI.getPort()), this.conf);

		this.host = HostUtils.determineHostname();
		if (this.host == null) {
			throw new IllegalStateException("Cannot determine hostname");
		}
		LOG.info("Determined hostname of datanode: " + this.host);

		// Initialization of buffer pool at the beginning
		BufferPool.initialize(this.nameNode, this.host);

		// Register with name node
		this.nameNode.registerDataNode(this.host, CDFS.DATANODE_DATA_PORT);

		// Create and store reference to HDFS file system
		this.hdfs = new Path(hdfsURI).getFileSystem(conf);

		// Start the prefetcher thread
		this.blockPrefetcher = new BlockPrefetcher(this, this.nameNode);
	}

	void run() throws IOException {

		while (true) {

			final Socket socket = this.serverSocket.accept();

			this.activeConnectons.add(new Connection(socket, this.nameNode, this.conf, this.host, this.hdfs,
				this.pathConverter, this));
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

		if (this.blockPrefetcher != null) {
			this.blockPrefetcher.shutDown();
		}

		if (this.hdfs != null) {
			try {
				this.hdfs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeConnection(final Connection connection) {

		this.activeConnectons.remove(connection);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasActiveConnections() {

		return !this.activeConnectons.isEmpty();
	}
}
