package edu.berkeley.icsi.cdfs.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final class MetaDataStore {

	private final String storageLocation;

	private final Map<Path, FileMetaData> metaData = new HashMap<Path, FileMetaData>();

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

			System.out.println("RESTORED " + fmd.getPath());
			this.metaData.put(fmd.getPath(), fmd);
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

		if (this.metaData.containsKey(path)) {
			return false;
		}

		final FileMetaData fmd = new FileMetaData(path);
		this.metaData.put(path, fmd);

		// Save meta data changes
		save(fmd);

		return true;
	}

	synchronized void addNewBlock(final Path cdfsPath, final Path hdfsPath, final int blockIndex, final int blockLength)
			throws IOException {

		final FileMetaData fmd = this.metaData.get(cdfsPath);
		fmd.addNewBlock(hdfsPath, blockIndex, blockLength);

		// Save meta data changes
		save(fmd);
	}

	synchronized FileStatus getFileStatus(final Path path) {

		return null;
	}
}
