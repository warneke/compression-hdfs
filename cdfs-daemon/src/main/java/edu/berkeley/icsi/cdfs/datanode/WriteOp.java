package edu.berkeley.icsi.cdfs.datanode;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Compressor;
import edu.berkeley.icsi.cdfs.sharedmem.SharedMemoryConsumer;
import edu.berkeley.icsi.cdfs.utils.ConfigUtils;

final class WriteOp implements Closeable {

	private final FileSystem hdfs;

	private final Configuration conf;

	private final Compressor compressor;

	private final SharedMemoryConsumer sharedMemoryConsumer;

	private List<Buffer> uncompressedBuffers = null;

	private List<Buffer> compressedBuffers = null;

	private int bytesWrittenInBlock = 0;

	private boolean cacheUncompressed;

	private boolean cacheCompressed;

	WriteOp(final DatagramSocket socket, final FileSystem hdfs, final Configuration conf) throws IOException {

		this.hdfs = hdfs;
		this.conf = conf;
		this.sharedMemoryConsumer = new SharedMemoryConsumer(socket);
		this.compressor = new Compressor(BufferPool.BUFFER_SIZE);

		this.cacheUncompressed = conf.getBoolean(ConfigUtils.ENABLE_UNCOMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_UNCOMPRESSED_CACHING);

		this.cacheCompressed = conf.getBoolean(ConfigUtils.ENABLE_COMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_COMPRESSED_CACHING);
	}

	boolean write(final Path hdfsPath, final int blockSize) throws IOException {

		// Create new cache lists
		this.uncompressedBuffers = new ArrayList<Buffer>();
		this.compressedBuffers = new ArrayList<Buffer>();

		System.out.println("Write to " + hdfsPath);
		boolean readEOF = false;

		BufferPool bufferPool = BufferPool.get();

		int readBytes = 0;
		while (readBytes < blockSize) {

			final ByteBuffer sharedBuffer = this.sharedMemoryConsumer.lockSharedMemory();
			if (sharedBuffer == null) {
				readEOF = true;
				break;
			}

			// Check out how much data we can read before we cross the block boundary
			final int bytesToRead = Math.min(sharedBuffer.remaining(), blockSize - readBytes);

			if (this.cacheUncompressed) {

				// Get buffer to cache
				final byte[] uncompressedBuffer = bufferPool.lockBuffer();
				if (uncompressedBuffer != null) {
					sharedBuffer.get(uncompressedBuffer, 0, bytesToRead);
					final Buffer buffer = new Buffer(uncompressedBuffer, bytesToRead);
					this.uncompressedBuffers.add(buffer);
				} else {
					clearUncompressedBuffers();
					this.cacheUncompressed = false;
				}
			}

			readBytes += bytesToRead;

			if (!sharedBuffer.hasRemaining()) {
				this.sharedMemoryConsumer.unlockSharedMemory();
			}
		}

		this.bytesWrittenInBlock = readBytes;

		System.out.println("Finished block after " + readBytes + " " + readEOF);

		return readEOF;
	}

	List<Buffer> getUncompressedBuffers() {

		return Collections.unmodifiableList(this.uncompressedBuffers);
	}

	List<Buffer> getCompressedBuffers() {

		return Collections.unmodifiableList(this.compressedBuffers);
	}

	int getBytesWrittenInBlock() {

		return this.bytesWrittenInBlock;
	}

	private final void clearUncompressedBuffers() {

		final Iterator<Buffer> it = this.uncompressedBuffers.iterator();
		while (it.hasNext()) {
			BufferPool.get().releaseBuffer(it.next().getData());
		}

		this.uncompressedBuffers.clear();
	}

	private final void clearCompressedBuffers() {

		final Iterator<Buffer> it = this.compressedBuffers.iterator();
		while (it.hasNext()) {
			BufferPool.get().releaseBuffer(it.next().getData());
		}

		this.compressedBuffers.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.sharedMemoryConsumer.close();
	}
}
