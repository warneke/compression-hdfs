package edu.berkeley.icsi.cdfs.datanode;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class ReadOp implements Closeable {

	private final SharedMemoryProducer sharedMemoryProducer;

	private final Configuration conf;

	private List<Buffer> uncompressedBuffers = null;

	private List<Buffer> compressedBuffers = null;

	ReadOp(final DatagramSocket socket, final SocketAddress remoteAddress, final Configuration conf) throws IOException {
		this.sharedMemoryProducer = new SharedMemoryProducer(socket, remoteAddress);
		this.conf = conf;
	}

	public void readFromCacheUncompressed(final List<Buffer> uncompressedBuffers) throws IOException {

		final Iterator<Buffer> it = uncompressedBuffers.iterator();
		while (it.hasNext()) {
			final Buffer buffer = it.next();

			final ByteBuffer byteBuffer = this.sharedMemoryProducer.lockSharedMemory();
			byteBuffer.put(buffer.getData(), 0, buffer.getLength());
			this.sharedMemoryProducer.unlockSharedMemory();
		}
	}

	public void readFromCacheCompressed(final List<Buffer> compressedBuffers) throws IOException {

		boolean cacheUncompressed = conf.getBoolean(ConfigUtils.ENABLE_UNCOMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_UNCOMPRESSED_CACHING);

		this.uncompressedBuffers = new ArrayList<Buffer>();

		final Decompressor decompressor = new Decompressor();
		final BufferPool bufferPool = BufferPool.get();

		byte[] uncompressedBuffer = null;

		if (!cacheUncompressed) {
			uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}

		final Iterator<Buffer> it = compressedBuffers.iterator();
		while (it.hasNext()) {

			final Buffer buffer = it.next();
			int offset = 0;

			while (offset < buffer.getLength()) {
				final int numberOfCompressedBytes = NumberUtils.byteArrayToInteger(buffer.getData(), offset);
				offset += 4;

				if (cacheUncompressed) {
					uncompressedBuffer = bufferPool.lockBuffer();
					if (uncompressedBuffer == null) {
						uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
						cacheUncompressed = false;
						clearUncompressedBuffers();
					}
				}

				final int numberOfUncompressedBytes = decompressor.decompress(buffer.getData(), offset,
					numberOfCompressedBytes, uncompressedBuffer);
				offset += numberOfCompressedBytes;

				final ByteBuffer byteBuffer = this.sharedMemoryProducer.lockSharedMemory();
				byteBuffer.put(uncompressedBuffer, 0, numberOfUncompressedBytes);
				this.sharedMemoryProducer.unlockSharedMemory();

				if (cacheUncompressed) {
					final Buffer ub = new Buffer(uncompressedBuffer, numberOfUncompressedBytes);
					this.uncompressedBuffers.add(ub);
				}
			}
		}
	}

	public void readFromHDFSCompressed(final FileSystem hdfs, final Path hdfsPath) throws IOException {

		boolean cacheUncompressed = conf.getBoolean(ConfigUtils.ENABLE_UNCOMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_UNCOMPRESSED_CACHING);
		boolean cacheCompressed = conf.getBoolean(ConfigUtils.ENABLE_COMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_COMPRESSED_CACHING);

		this.uncompressedBuffers = new ArrayList<Buffer>();
		this.compressedBuffers = new ArrayList<Buffer>();

		byte[] compressedBuffer = null;
		byte[] uncompressedBuffer = null;

		if (!cacheCompressed) {
			compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}
		if (!cacheUncompressed) {
			uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}

		final BufferPool bufferPool = BufferPool.get();

		final Decompressor decompressor = new Decompressor();

		final FSDataInputStream hdfsInputStream = hdfs.open(hdfsPath);

		final byte[] lenBuf = new byte[4];

		int numberOfBytesInCompressedBuffer = 0;

		while (true) {

			int r = hdfsInputStream.read(lenBuf, 0, lenBuf.length);
			if (r < 0) {
				break;
			}

			if (r != 4) {
				throw new IllegalArgumentException("Short read on lenBuf");
			}

			final int bytesToReadFromHDFS = NumberUtils.byteArrayToInteger(lenBuf, 0);

			// Make sure we have a buffer to copy the compressed data to
			while (true) {
				// We still have an compressed buffer
				if (compressedBuffer != null) {
					if (numberOfBytesInCompressedBuffer + bytesToReadFromHDFS + 4 <= compressedBuffer.length) {
						// There is enough memory left in the buffer, we can write to it
						break;
					} else {
						// The buffer is full, see if we need to cache it
						if (cacheCompressed) {
							final Buffer buffer = new Buffer(compressedBuffer, numberOfBytesInCompressedBuffer);
							this.compressedBuffers.add(buffer);
							compressedBuffer = null;
						}
						numberOfBytesInCompressedBuffer = 0;
					}
				} else {
					if (cacheCompressed) {
						compressedBuffer = bufferPool.lockBuffer();
						if (compressedBuffer == null) {
							compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
							clearCompressedBuffers();
							cacheCompressed = false;
						}
						break;
					} else {
						System.out.println("Illegal state!!!!");
					}
				}
			}

			// Copy length field
			System.arraycopy(lenBuf, 0, compressedBuffer, numberOfBytesInCompressedBuffer, 4);
			numberOfBytesInCompressedBuffer += 4;
			// Copy actual data
			hdfsInputStream.readFully(compressedBuffer, numberOfBytesInCompressedBuffer, bytesToReadFromHDFS);

			if (cacheUncompressed) {
				uncompressedBuffer = bufferPool.lockBuffer();
				if (uncompressedBuffer == null) {
					uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
					clearUncompressedBuffers();
					cacheUncompressed = false;
				}
			}

			// Decompress the data
			final int numberOfUncompressedBytes = decompressor.decompress(compressedBuffer,
				numberOfBytesInCompressedBuffer, bytesToReadFromHDFS, uncompressedBuffer);

			final ByteBuffer sharedBuffer = this.sharedMemoryProducer.lockSharedMemory();
			sharedBuffer.put(uncompressedBuffer, 0, numberOfUncompressedBytes);
			this.sharedMemoryProducer.unlockSharedMemory();

			if (cacheUncompressed) {
				final Buffer buffer = new Buffer(uncompressedBuffer, numberOfUncompressedBytes);
				this.uncompressedBuffers.add(buffer);
			}

			numberOfBytesInCompressedBuffer += bytesToReadFromHDFS;
		}

		// Check if we have to cache the last compressed buffer
		if (numberOfBytesInCompressedBuffer > 0 && cacheCompressed) {
			final Buffer buffer = new Buffer(compressedBuffer, numberOfBytesInCompressedBuffer);
			this.compressedBuffers.add(buffer);
		}

		// Clean up
		hdfsInputStream.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.sharedMemoryProducer.close();
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
}
