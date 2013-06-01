package edu.berkeley.icsi.cdfs.conf;

public class ConfigConstants {

	private ConfigConstants() {
	}

	/**
	 * The size of the buffers in bytes.
	 */
	public static final int BUFFER_SIZE = 2 * 1024 * 1024;

	/**
	 * The block size in bytes.
	 */
	public static final int BLOCK_SIZE = 256 * 1024 * 1024;

	/**
	 * The block replication.
	 */
	public static final int BLOCK_REPLICATION = 3;

	public static final String HDFS_DEFAULT_NAME_KEY = "hdfs.default.name";

	public static final String DEFEAULT_HDFS_DEFAULT_NAME = "hdfs://localhost:9000/";

	public static final String CDFS_DEFAULT_NAME_KEY = "cdfs.default.name";

	public static final String DEFEAULT_CDFS_DEFAULT_NAME = "cdfs://localhost:8000/";

	public static final String ENABLE_UNCOMPRESSED_CACHING_KEY = "cache.uncompressed.enable";

	public static final boolean DEFAULT_ENABLE_UNCOMPRESSED_CACHING = false;

	public static final String ENABLE_COMPRESSED_CACHING_KEY = "cache.compressed.enable";

	public static final boolean DEFAULT_ENABLE_COMPRESSED_CACHING = false;
}
