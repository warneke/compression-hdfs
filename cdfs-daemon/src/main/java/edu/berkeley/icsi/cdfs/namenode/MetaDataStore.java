package edu.berkeley.icsi.cdfs.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import edu.berkeley.icsi.cdfs.BlockReadInformation;
import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.CDFSBlockLocation;
import edu.berkeley.icsi.cdfs.PopularFile;
import edu.berkeley.icsi.cdfs.cache.EvictionEntry;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.utils.HostUtils;
import edu.berkeley.icsi.cdfs.utils.PathConverter;

final class MetaDataStore {

	private static final Log LOG = LogFactory.getLog(MetaDataStore.class);

	private final FileSystem hdfs;

	private final PathConverter pathConverter;

	private final String storageLocation;

	private final Map<String, FileMetaData> metaData = new HashMap<String, FileMetaData>();

	private final Map<String, HostCacheData> hostCacheData = new HashMap<String, HostCacheData>();

	private final FileAccessList fileAccessList = new FileAccessList();

	private final Kryo kryo = new Kryo();

	public MetaDataStore(final FileSystem hdfs, final PathConverter pathConverter) throws IOException {

		this.hdfs = hdfs;
		this.pathConverter = pathConverter;

		String userName = System.getProperty("user.name");
		if (userName == null) {
			userName = "default";
		}

		this.storageLocation = "/tmp/cdfs-" + userName;
		new File(this.storageLocation).mkdirs();
		loadMetaData();
	}

	private void loadMetaData() throws IOException {

		final File dir = new File(this.storageLocation);

		for (final File file : dir.listFiles()) {

			final Input input = new Input(new FileInputStream(file));
			final FileMetaData fmd = this.kryo.readObject(input, FileMetaData.class);
			input.close();

			this.metaData.put(fmd.getPath().toUri().getPath(), fmd);
		}
	}

	private void save(final FileMetaData fmd) throws IOException {

		final Path path = fmd.getPath();
		final File file = new File(this.storageLocation + File.separator + path.toUri().getPath().replace("/", ""));

		final Output output = new Output(new FileOutputStream(file));
		this.kryo.writeObject(output, fmd);
		output.close();
	}

	synchronized boolean create(final Path path, final boolean overwrite) throws IOException {

		final String key = path.toUri().getPath();

		FileMetaData fmd;

		if (overwrite) {
			// Delete file if it already exists
			fmd = this.metaData.remove(key);
			if (fmd != null) {
				synchronized (this.hdfs) {
					final Iterator<BlockMetaData> it = fmd.getBlockIterator();
					while (it.hasNext()) {
						final BlockMetaData bmd = it.next();
						this.hdfs.delete(bmd.getHdfsPath(), false);
					}
				}
			}

		} else {
			// Return with error if file already exists
			if (this.metaData.containsKey(key)) {
				return false;
			}
		}

		// Create new meta data
		fmd = new FileMetaData(path);
		this.metaData.put(path.toUri().getPath(), fmd);

		// Save meta data changes
		save(fmd);

		return true;
	}

	synchronized void addNewBlock(final Path cdfsPath, final Path hdfsPath, final int blockIndex, final int blockLength)
			throws IOException {

		final FileMetaData fmd = this.metaData.get(cdfsPath.toUri().getPath());
		fmd.addNewBlock(hdfsPath, blockIndex, blockLength);

		// Save meta data changes
		save(fmd);
	}

	synchronized FileStatus getFileStatus(final Path path) {

		final FileMetaData fmd = this.metaData.get(path.toUri().getPath());
		if (fmd == null) {
			return null;
		}

		final FileStatus fs = new FileStatus(fmd.getLength(), false, ConfigConstants.BLOCK_REPLICATION,
			ConfigConstants.BLOCK_SIZE, fmd.getModificationTime(), fmd.getPath());

		return fs;
	}

	synchronized CDFSBlockLocation[] getFileBlockLocations(final Path path, final long start, final long len)
			throws IOException {

		final FileMetaData fmd = this.metaData.get(path.toUri().getPath());
		if (fmd == null) {
			return null;
		}

		final BlockMetaData[] blocks = fmd.getBlockMetaData(start, len);
		if (blocks == null) {
			return null;
		}

		// Construct host priorities
		final CDFSBlockLocation[] blockLocations = new CDFSBlockLocation[blocks.length];
		for (int i = 0; i < blocks.length; ++i) {

			final Path hdfsPath = this.pathConverter.convert(path, "_" + blocks[i].getIndex());
			BlockLocation[] hdfsBlockLocations;
			synchronized (this.hdfs) {
				final FileStatus fileStatus = this.hdfs.getFileStatus(hdfsPath);
				hdfsBlockLocations = this.hdfs.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());
			}
			if (hdfsBlockLocations == null) {
				throw new IllegalStateException("Could not retrieve block locations for " + hdfsPath);
			}

			if (hdfsBlockLocations.length != 1) {
				throw new IllegalStateException(hdfsPath + " spreads across " + blockLocations.length + " blocks");
			}

			final String hosts[] = blocks[i].constructHostList(hdfsBlockLocations[0].getHosts());
			final String names[] = new String[hosts.length];
			for (int j = 0; j < hosts.length; ++j) {
				names[j] = hosts[j] + ":" + CDFS.DATANODE_DATA_PORT;
			}

			blockLocations[i] = new CDFSBlockLocation(blocks[i].getIndex(), names, hosts, blocks[i].getOffset(),
				blocks[i].getLength());

			if (LOG.isInfoEnabled()) {
				LOG.info("Constructed " + fmd.getPath() + " " + blockLocations[i]);
			}
		}
		return blockLocations;
	}

	synchronized BlockReadInformation[] getBlockReadInformation(final Path path, final long start, final long len) {

		final FileMetaData fmd = this.metaData.get(path.toUri().getPath());
		if (fmd == null) {
			return null;
		}

		// Increase access count
		this.fileAccessList.increaseAccessCount(fmd);

		final BlockMetaData[] blocks = fmd.getBlockMetaData(start, len);
		if (blocks == null) {
			return null;
		}

		final BlockReadInformation[] readInformation = new BlockReadInformation[blocks.length];
		final int numberOfBlocks = fmd.getNumberOfBlocks();

		for (int i = 0; i < blocks.length; ++i) {
			readInformation[i] = new BlockReadInformation(blocks[i].getIndex(), blocks[i].getOffset(),
				blocks[i].getLength(), numberOfBlocks, false, false);
		}

		return readInformation;
	}

	synchronized PopularFile[] getPopularFiles(final int maximumNumberOfFiles) {

		return null;
	}

	synchronized void reportCachedBlock(final Path path, final int blockIndex, final boolean compressed,
			final String host) {

		final FileMetaData fmd = this.metaData.get(path.toUri().getPath());
		if (fmd == null) {
			throw new IllegalStateException("Cannot find meta data for " + path);
		}

		final BlockMetaData bmd = fmd.addCachedBlock(blockIndex, host, compressed);

		// Update host view
		HostCacheData hcd = this.hostCacheData.get(host);
		if (hcd == null) {
			hcd = new HostCacheData(this.fileAccessList);
			this.hostCacheData.put(host, hcd);
		}
		hcd.add(fmd, bmd, compressed);
	}

	synchronized void confirmEviction(final Path path, final int blockIndex, final boolean compressed, final String host) {

		final FileMetaData fmd = this.metaData.get(path.toUri().getPath());
		if (fmd == null) {
			throw new IllegalStateException("Cannot find meta data for " + path);
		}

		final BlockMetaData bmd = fmd.removeCachedBlock(blockIndex, host, compressed);
		// Update host view
		final HostCacheData hcd = this.hostCacheData.get(host);
		if (hcd == null) {
			throw new IllegalStateException("No cache data for host " + host);
		}
		hcd.remove(fmd, bmd, compressed);
	}

	synchronized EvictionEntry getFileToEvictLIFE(final String host) {

		final String strippedHost = HostUtils.stripFQDN(host);

		final HostCacheData hcd = this.hostCacheData.get(host);
		if (hcd == null) {
			throw new IllegalStateException("Evict: No host cache data for host " + host);
		}

		EvictionEntry ee = hcd.getLargestUncompressedIncompleteFile();
		if (ee != null) {
			LOG.info("LIFE: Chose to evict " + ee.getPathWrapper().getPath() + " (uncompressed, incomplete) at "
				+ strippedHost);
			return ee;
		}

		ee = hcd.getLargestCompressedIncompleteFile();
		if (ee != null) {
			LOG.info("LIFE: Chose to evict " + ee.getPathWrapper().getPath() + " (compressed, incomplete) at "
				+ strippedHost);
			return ee;
		}

		ee = hcd.getLargestUncompressedCompleteFile();
		if (ee != null) {
			LOG.info("LIFE: Chose to evict " + ee.getPathWrapper().getPath() + " (uncompressed, complete) at "
				+ strippedHost);
			return ee;
		}

		ee = hcd.getLargestCompressedCompleteFile();
		if (ee != null) {
			LOG.info("LIFE: Chose to evict " + ee.getPathWrapper().getPath() + " (compressed, complete) at "
				+ strippedHost);
			return ee;
		}

		throw new IllegalStateException("LIFE: No file to evict from host " + strippedHost);
	}

	synchronized EvictionEntry getFileToEvictLFUF(final String host) {

		final String strippedHost = HostUtils.stripFQDN(host);

		final HostCacheData hcd = this.hostCacheData.get(host);
		if (hcd == null) {
			throw new IllegalStateException("Evict: No host cache data for host " + host);
		}

		EvictionEntry ee = hcd.getLeastAccessedUncompressedIncompleteFile();
		if (ee != null) {
			LOG.info("LFU-F: Chose to evict " + ee.getPathWrapper().getPath() + " (uncompressed, incomplete) at "
				+ strippedHost);
			return ee;
		}

		ee = hcd.getLeastAccessedCompressedIncompleteFile();
		if (ee != null) {
			LOG.info("LFU-F: Chose to evict " + ee.getPathWrapper().getPath() + " (compressed, incomplete) at "
				+ strippedHost);
			return ee;
		}

		ee = hcd.getLeastAccessedUncompressedCompleteFile();
		if (ee != null) {
			LOG.info("LFU-F: Chose to evict " + ee.getPathWrapper().getPath() + " (uncompressed, complete) at "
				+ strippedHost);
			return ee;
		}

		ee = hcd.getLeastAccessedCompressedCompleteFile();
		if (ee != null) {
			LOG.info("LFU-F: Chose to evict " + ee.getPathWrapper().getPath() + " (compressed, complete) at "
				+ strippedHost);
			return ee;
		}

		throw new IllegalStateException("LFU-F: No file to evict from host " + strippedHost);
	}
}
