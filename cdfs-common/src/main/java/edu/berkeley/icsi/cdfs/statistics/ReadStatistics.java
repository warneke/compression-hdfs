package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public final class ReadStatistics implements AbstractStatistics {

	private static final byte DISK_READ = 0;

	private static final byte CACHED_COMPRESSED_READ = 1;

	private static final byte CACHED_UNCOMPRESSED_READ = 2;

	private PathWrapper path;

	private int index;

	private byte readMode;

	private long time;

	private ReadStatistics(final Path path, final int index, final byte readMode, final long time) {

		this.path = new PathWrapper(path);
		this.index = index;
		this.readMode = readMode;
		this.time = time;
	}

	public ReadStatistics() {
	}

	public static ReadStatistics createDisk(final Path path, final int index) {

		return new ReadStatistics(path, index, DISK_READ, System.currentTimeMillis());
	}

	public static ReadStatistics createCacheCompressed(final Path path, final int index) {

		return new ReadStatistics(path, index, CACHED_COMPRESSED_READ, System.currentTimeMillis());
	}

	public static ReadStatistics createCacheUncompressed(final Path path, final int index) {

		return new ReadStatistics(path, index, CACHED_UNCOMPRESSED_READ, System.currentTimeMillis());
	}

	public Path getPath() {
		return this.path.getPath();
	}

	public int getIndex() {
		return this.index;
	}

	public boolean isDiskRead() {

		return this.readMode == DISK_READ;
	}

	public boolean isCachedCompressedRead() {

		return this.readMode == CACHED_COMPRESSED_READ;
	}

	public boolean isCachedUncompressedRead() {

		return this.readMode == CACHED_UNCOMPRESSED_READ;
	}

	public long getTime() {

		return this.time;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		this.path = new PathWrapper();
		this.path.readFields(arg0);
		this.index = arg0.readInt();
		this.readMode = arg0.readByte();
		this.time = arg0.readLong();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		this.path.write(arg0);
		arg0.writeInt(this.index);
		arg0.writeByte(this.readMode);
		arg0.writeLong(this.time);
	}
}
