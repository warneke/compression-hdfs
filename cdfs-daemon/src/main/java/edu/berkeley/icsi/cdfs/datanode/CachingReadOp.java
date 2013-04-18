package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Decompressor;
import edu.berkeley.icsi.cdfs.sharedmem.SharedMemoryProducer;
import edu.berkeley.icsi.cdfs.utils.ConfigUtils;

final class CachingReadOp extends AbstractReadOp {

	private final FileSystem hdfs;

	private final Path hdfsPath;

	private final Decompressor decompressor;

	private boolean cacheUncompressed;

	private boolean cacheCompressed;

	private final List<Buffer> uncompressedBuffers;

	private final List<Buffer> compressedBuffers;

	CachingReadOp(final FileSystem hdfs, final Path hdfsPath, final Configuration conf) {

		this.hdfs = hdfs;
		this.hdfsPath = hdfsPath;
		this.decompressor = new Decompressor(BufferPool.BUFFER_SIZE);

		this.cacheUncompressed = conf.getBoolean(ConfigUtils.ENABLE_UNCOMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_UNCOMPRESSED_CACHING);

		this.cacheCompressed = conf.getBoolean(ConfigUtils.ENABLE_COMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_COMPRESSED_CACHING);

		this.uncompressedBuffers = new ArrayList<Buffer>();
		this.compressedBuffers = new ArrayList<Buffer>();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	void read(final SocketAddress remoteAddress) throws IOException {

		byte[] compressedBuffer = null;

		byte[] uncompressedBuffer = null;

		int numberOfBytesInCompressedBuffer = 0;

		final BufferPool bufferPool = BufferPool.get();

		FSDataInputStream hdfsInputStream = null;

		SharedMemoryProducer smp = null;

		if (!this.cacheCompressed) {
			compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}

		if (!this.cacheUncompressed) {
			uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}

		try {

			hdfsInputStream = this.hdfs.open(this.hdfsPath);

			smp = new SharedMemoryProducer(remoteAddress);

			while (true) {

				int read;

				while (true) {

					if (this.cacheCompressed) {
						compressedBuffer = bufferPool.lockBuffer();
						if (compressedBuffer == null) {
							clearCompressedBuffers();
							this.cacheCompressed = false;
							compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
						}
					}

					read = hdfsInputStream.read(compressedBuffer, numberOfBytesInCompressedBuffer,
						compressedBuffer.length - numberOfBytesInCompressedBuffer);
					if (read < 0) {
						break;
					}

					numberOfBytesInCompressedBuffer += read;

					if (numberOfBytesInCompressedBuffer == compressedBuffer.length) {
						break;
					}
				}

				if (this.cacheCompressed) {
					System.out.println("Caching compressed buffer " + numberOfBytesInCompressedBuffer);
				}

				if (this.cacheUncompressed) {
					uncompressedBuffer = bufferPool.lockBuffer();
					if (uncompressedBuffer == null) {
						clearUncompressedBuffers();
						this.cacheUncompressed = false;
						uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
					}
				}

				// Decompress here
				int numberOfUncompressedBytes = this.decompressor.decompress(compressedBuffer,
					numberOfBytesInCompressedBuffer, uncompressedBuffer);

				if (numberOfUncompressedBytes < 0) {
					bufferPool.releaseBuffer(uncompressedBuffer);
				} else {
					System.out.println("Caching uncompresed buffer " + numberOfUncompressedBytes);
				}

				while (numberOfUncompressedBytes > 0) {

					// Send data back to client
					final ByteBuffer buffer = smp.lockSharedMemory();
					buffer.put(uncompressedBuffer, 0, numberOfUncompressedBytes);
					smp.unlockSharedMemory();

					if (this.cacheUncompressed) {
						uncompressedBuffer = bufferPool.lockBuffer();
						if (uncompressedBuffer == null) {
							clearUncompressedBuffers();
							this.cacheUncompressed = false;
							uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
						}
					}

					numberOfUncompressedBytes = this.decompressor.decompress(compressedBuffer,
						numberOfBytesInCompressedBuffer, uncompressedBuffer);

					if (this.cacheUncompressed) {
						if (numberOfUncompressedBytes < 0) {
							bufferPool.releaseBuffer(uncompressedBuffer);
						} else {
							System.out.println("Caching uncompresed buffer2 " + numberOfUncompressedBytes);
						}
					}
				}

				numberOfBytesInCompressedBuffer = 0;

				if (read < 0) {
					break;
				}
			}
		} finally {

			// Close the shared memory producer
			if (smp != null) {
				smp.close();
			}

			// Close the input stream
			if (hdfsInputStream != null) {
				hdfsInputStream.close();
			}
		}
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

	List<Buffer> getUncompressedBuffers() {

		return Collections.unmodifiableList(this.uncompressedBuffers);
	}

	List<Buffer> getCompressedBuffers() {

		return Collections.unmodifiableList(this.compressedBuffers);
	}
}
