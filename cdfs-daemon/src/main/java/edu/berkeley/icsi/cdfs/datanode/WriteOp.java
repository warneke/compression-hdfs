package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Compressor;
import edu.berkeley.icsi.cdfs.utils.ConfigUtils;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class WriteOp {

	private final int blockSize;

	private final Path hdfsPath;

	private final FileSystem hdfs;

	private final Compressor compressor;

	private FSDataOutputStream hdfsOutputStream = null;

	private boolean cacheUncompressed;

	private boolean cacheCompressed;

	private byte[] uncompressedBuffer = null;

	private byte[] compressedBuffer = null;

	private int numberOfBytesInUncompressedBuffer = 0;

	private int numberOfBytesInCompressedBuffer = 0;

	private int bytesWrittenInBlock = 0;

	private final List<Buffer> uncompressedBuffers;

	private final List<Buffer> compressedBuffers;

	WriteOp(final FileSystem hdfs, final Path hdfsPath, final int blockSize, final Configuration conf) {

		this.hdfs = hdfs;
		this.hdfsPath = hdfsPath;
		this.blockSize = blockSize;
		this.compressor = new Compressor(BufferPool.BUFFER_SIZE);

		this.cacheUncompressed = conf.getBoolean(ConfigUtils.ENABLE_UNCOMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_UNCOMPRESSED_CACHING);
		if (!this.cacheUncompressed) {
			this.uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}

		this.cacheCompressed = conf.getBoolean(ConfigUtils.ENABLE_COMPRESSED_CACHING_KEY,
			ConfigUtils.DEFAULT_ENABLE_COMPRESSED_CACHING);
		if (!this.cacheCompressed) {
			this.compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
		}

		this.uncompressedBuffers = new ArrayList<Buffer>();
		this.compressedBuffers = new ArrayList<Buffer>();
	}

	private final void writeToHDFS() throws IOException {

		if (this.numberOfBytesInCompressedBuffer > 0) {
			this.hdfsOutputStream.write(this.compressedBuffer, 0, this.numberOfBytesInCompressedBuffer);
		}
	}

	private void swapUncompressedBuffer() {

		if (this.cacheUncompressed) {

			if (this.uncompressedBuffer != null) {

				final Buffer buffer = new Buffer(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer);
				this.uncompressedBuffers.add(buffer);
				this.numberOfBytesInUncompressedBuffer = 0;
			}

			this.uncompressedBuffer = BufferPool.get().lockBuffer();
			if (this.uncompressedBuffer == null) {
				clearUncompressedBuffers();
				this.uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
				this.cacheUncompressed = false;
			}
		} else {
			this.numberOfBytesInUncompressedBuffer = 0;
		}
	}

	private void swapCompressedBuffer() throws IOException {

		if (this.cacheCompressed) {

			if (this.compressedBuffer != null) {

				if (this.compressedBuffer.length - this.numberOfBytesInCompressedBuffer >= 4
					&& this.bytesWrittenInBlock != this.blockSize) {
					return;
				}

				writeToHDFS();

				final Buffer buffer = new Buffer(this.compressedBuffer, this.numberOfBytesInCompressedBuffer);
				this.compressedBuffers.add(buffer);
				this.numberOfBytesInCompressedBuffer = 0;
			}

			this.compressedBuffer = BufferPool.get().lockBuffer();
			if (this.compressedBuffer == null) {

				if (this.cacheUncompressed) {
					clearUncompressedBuffers();
					if (this.numberOfBytesInUncompressedBuffer > 0) {
						final byte[] newBuf = new byte[BufferPool.BUFFER_SIZE];
						System.arraycopy(this.uncompressedBuffer, 0, newBuf, 0, this.numberOfBytesInUncompressedBuffer);
						BufferPool.get().releaseBuffer(this.uncompressedBuffer);
						this.uncompressedBuffer = newBuf;
					}
					this.cacheCompressed = false;
				}

				this.compressedBuffer = BufferPool.get().lockBuffer();
				if (this.compressedBuffer == null) {
					if (this.cacheCompressed) {
						clearCompressedBuffers();
						this.compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
						this.cacheCompressed = false;
					}
				}
			}
		} else {
			writeToHDFS();
			this.numberOfBytesInCompressedBuffer = 0;
		}
	}

	boolean write(final InputStream inputStream) throws IOException {

		boolean readEOF = false;

		try {

			this.hdfsOutputStream = this.hdfs.create(this.hdfsPath, true);

			while (true) {

				// Get a new uncompressed buffer
				swapUncompressedBuffer();

				// Read the complete block
				int r;
				while (true) {

					final int bytesToRead = Math.min(this.uncompressedBuffer.length
						- this.numberOfBytesInUncompressedBuffer, this.blockSize - this.bytesWrittenInBlock);

					r = inputStream.read(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer, bytesToRead);
					if (r < 0) {
						readEOF = true;
						break;
					}

					this.numberOfBytesInUncompressedBuffer += r;
					this.bytesWrittenInBlock += r;

					// Buffer is full
					if (this.numberOfBytesInUncompressedBuffer == this.uncompressedBuffer.length) {
						break;
					}

					// Block limit is reached
					if (this.bytesWrittenInBlock == this.blockSize) {
						break;
					}
				}

				if (this.numberOfBytesInUncompressedBuffer == 0 && this.bytesWrittenInBlock != this.blockSize) {
					if (this.cacheUncompressed) {
						BufferPool.get().releaseBuffer(this.uncompressedBuffer);
						this.uncompressedBuffer = null;
					}
					break;
				}

				// System.out.println("WRITE " + this.numberOfBytesInUncompressedBuffer);

				// Compress the block
				final int numberOfCompressedBytes = this.compressor.compress(this.uncompressedBuffer,
					this.numberOfBytesInUncompressedBuffer);

				// System.out.println("COMPRESSED " + numberOfCompressedBytes);

				swapCompressedBuffer();

				NumberUtils.integerToByteArray(numberOfCompressedBytes, this.compressedBuffer,
					this.numberOfBytesInCompressedBuffer);
				this.numberOfBytesInCompressedBuffer += 4;

				r = 0;
				while (r < numberOfCompressedBytes) {

					final int bytesToCopy = Math.min(numberOfCompressedBytes - r, this.compressedBuffer.length
						- this.numberOfBytesInCompressedBuffer);
					System.arraycopy(this.compressor.getCompressedBuffer(), r, this.compressedBuffer,
						this.numberOfBytesInCompressedBuffer, bytesToCopy);

					r += bytesToCopy;
					this.numberOfBytesInCompressedBuffer += bytesToCopy;

					swapCompressedBuffer();
				}

				if (this.bytesWrittenInBlock == this.blockSize || readEOF) {
					break;
				}
			}

			if (this.uncompressedBuffer != null && this.cacheUncompressed) {
				final Buffer buffer = new Buffer(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer);
				this.uncompressedBuffers.add(buffer);
				this.numberOfBytesInUncompressedBuffer = 0;
				this.uncompressedBuffer = null;
			}

			if (this.compressedBuffer != null) {

				writeToHDFS();

				if (this.cacheCompressed) {
					final Buffer buffer = new Buffer(this.compressedBuffer, this.numberOfBytesInCompressedBuffer);
					this.compressedBuffers.add(buffer);
					this.numberOfBytesInCompressedBuffer = 0;
					this.compressedBuffer = null;
				}
			}

		} finally {

			if (this.hdfsOutputStream != null) {
				this.hdfsOutputStream.close();
			}
		}

		return readEOF;
	}

	public int getBytesWrittenInBlock() {

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

	List<Buffer> getUncompressedBuffers() {

		return Collections.unmodifiableList(this.uncompressedBuffers);
	}

	List<Buffer> getCompressedBuffers() {

		return Collections.unmodifiableList(this.compressedBuffers);
	}
}
