package edu.berkeley.icsi.cdfs.namenode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.BlockReadInformation;
import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.ConnectionInfo;
import edu.berkeley.icsi.cdfs.PopularFile;
import edu.berkeley.icsi.cdfs.cache.EvictionEntry;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.conf.ConfigUtils;
import edu.berkeley.icsi.cdfs.protocols.ClientNameNodeProtocol;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.statistics.AbstractUserStatistics;
import edu.berkeley.icsi.cdfs.statistics.ReadStatistics;
import edu.berkeley.icsi.cdfs.utils.PathConverter;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;
import edu.berkeley.icsi.cdfs.utils.WritableArrayList;

public class NameNode implements ClientNameNodeProtocol, DataNodeNameNodeProtocol {

	private static final Log LOG = LogFactory.getLog(NameNode.class);

	private final PathConverter pathConverter;

	private final Server rpcServer;

	private final MetaDataStore metaDataStore;

	private final StatisticsCollector statisticsCollector;

	private final FileSystem hdfs;

	private final Map<String, ConnectionInfo> registeredDataNodes = new ConcurrentHashMap<String, ConnectionInfo>();

	private NameNode(final Configuration conf) throws IOException {

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

		this.rpcServer = RPC.getServer(this, cdfsURI.getHost(), cdfsURI.getPort(), conf);
		this.rpcServer.start();

		this.hdfs = new Path(hdfsURI).getFileSystem(conf);

		this.metaDataStore = new MetaDataStore(this.hdfs, this.pathConverter, conf);
		this.statisticsCollector = new StatisticsCollector(conf);
	}

	public void shutDown() {
		this.rpcServer.stop();
		this.statisticsCollector.shutDown();
		this.metaDataStore.shutDown();
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

		final NameNode nameNode;

		try {
			nameNode = new NameNode(conf);
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
			return;
		}

		// Register shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {
				nameNode.shutDown();
			}

		});

		// Loop
		while (true) {
			try {
				Thread.sleep(5000L);
			} catch (InterruptedException ie) {
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
			final int uncompressedLength, final int compressedLength) throws IOException {

		this.metaDataStore.addNewBlock(cdfsPath.getPath(), hdfsPath.getPath(), blockIndex, uncompressedLength,
			compressedLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean mkdirs(final PathWrapper path, final FsPermission permission) throws IOException {

		// We don't care about the directory structure, so let HDFS handle this
		synchronized (this.hdfs) {
			return this.hdfs.mkdirs(this.pathConverter.convert(path.getPath(), ""), permission);
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
	public BlockReadInformation[] getBlockReadInformation(final PathWrapper path, final long start, final long len)
			throws IOException {

		return this.metaDataStore.getBlockReadInformation(path.getPath(), start, len);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportCachedBlock(final PathWrapper cdfsPath, final int blockIndex, final boolean compressed,
			final String host) throws IOException {

		this.metaDataStore.reportCachedBlock(cdfsPath.getPath(), blockIndex, compressed, host);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public EvictionEntry getFileToEvict(final String host) throws IOException {

		return this.metaDataStore.getFileToEvictLFUF(host);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void confirmEviction(final PathWrapper cdfsPath, final int blockIndex, final boolean compressed,
			final String host) throws IOException {

		this.metaDataStore.confirmEviction(cdfsPath.getPath(), blockIndex, compressed, host);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerDataNode(final String hostname, final int port) throws IOException {

		LOG.info("Registering data node " + hostname + " with port " + port);

		this.registeredDataNodes.put(hostname, new ConnectionInfo(hostname, port));

		TaskHistogram.registerHost(hostname);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ConnectionInfo determineClosestDataNode(final String hostname) throws IOException {

		ConnectionInfo ci = null;
		if (hostname != null) {
			ci = this.registeredDataNodes.get(hostname);
		}

		if (ci == null) {

			// Host does not have a local data node
			final Collection<ConnectionInfo> dataNodes = this.registeredDataNodes.values();
			final Iterator<ConnectionInfo> iterator = dataNodes.iterator();
			if (!iterator.hasNext()) {
				throw new IOException("No data node registered");
			}
			ci = iterator.next();
		}

		if (!hostname.equals(ci.getHostname())) {
			LOG.warn(hostname + " requested data node, returning " + ci.getHostname());
		}

		TaskHistogram.increaseHostCount(hostname);

		return ci;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportUserStatistics(final AbstractUserStatistics userStatistics) throws IOException {

		this.statisticsCollector.collectUserStatistics(userStatistics);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportReadStatistics(final WritableArrayList<ReadStatistics> readStatistics, final String host)
			throws IOException {

		this.statisticsCollector.collectReadStatistics(readStatistics);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PopularFile[] getPopularFiles(final int maximumNumberOfFiles) throws IOException {

		return this.metaDataStore.getPopularFiles(maximumNumberOfFiles);
	}
}
