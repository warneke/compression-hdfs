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
}
