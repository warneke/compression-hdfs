package edu.berkeley.icsi.cdfs.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.CDFSBlockLocation;

final class MetaDataStore {

	private static final Log LOG = LogFactory.getLog(MetaDataStore.class);

	private final String storageLocation;

	private final Map<String, FileMetaData> metaData = new HashMap<String, FileMetaData>();

	private final Kryo kryo = new Kryo();

	public MetaDataStore() throws IOException {

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

	synchronized boolean create(final Path path) throws IOException {

		if (this.metaData.containsKey(path.toUri().getPath())) {
			return false;
		}

		final FileMetaData fmd = new FileMetaData(path);
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

		final FileStatus fs = new FileStatus(fmd.getLength(), false, CDFS.BLOCK_REPLICATION, CDFS.BLOCK_SIZE,
			fmd.getModificationTime(), fmd.getPath());

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

		final String names[] = new String[1];
		names[0] = "localhost:" + CDFS.DATANODE_DATA_PORT;
		final String hosts[] = new String[1];
		hosts[0] = "localhost";

		final CDFSBlockLocation[] blockLocations = new CDFSBlockLocation[blocks.length];

		LOG.info("Number of block locations for " + path + " (start " + start + ", len " + len + "): " + blocks.length);

		for (int i = 0; i < blocks.length; ++i) {
			final CDFSBlockLocation blockLocation = new CDFSBlockLocation(blocks[i].getIndex(), names, hosts,
				blocks[i].getOffset(), blocks[i].getLength());
			blockLocations[i] = blockLocation;
		}

		return blockLocations;
	}

	private void reportCachedBlock(final Path path, final int blockIndex, final String host, final boolean compressed) {

		final FileMetaData fmd = this.metaData.get(path.toUri().getPath());
		if (fmd == null) {
			throw new IllegalStateException("Cannot find meta data for " + path);
		}

		fmd.addCachedBlock(blockIndex, host, compressed);
	}

	synchronized void reportUncompressedCachedBlock(final Path path, final int blockIndex, final String host) {

		reportCachedBlock(path, blockIndex, host, false);
	}

	synchronized void reportCompressedCachedBlock(final Path path, final int blockIndex, final String host) {

		reportCachedBlock(path, blockIndex, host, true);
	}
}
