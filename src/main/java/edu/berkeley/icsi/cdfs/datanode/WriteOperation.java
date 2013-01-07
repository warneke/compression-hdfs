package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;

import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Compressor;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class WriteOperation {

	private final BufferPool bufferPool;

	private final Compressor compressor;

	private final Path path;

	private boolean cacheUncompressed = true;

	private boolean cacheCompressed = true;

	private byte[] uncompressedBuffer = null;

	private byte[] compressedBuffer = null;

	private int numberOfBytesInUncompressedBuffer = 0;

	private int numberOfBytesInCompressedBuffer = 0;

	private final ArrayDeque<Buffer> uncompressedBuffers;

	private final ArrayDeque<Buffer> compressedBuffers;

	WriteOperation(final Path path) {

		this.bufferPool = BufferPool.get();
		this.compressor = new Compressor(BufferPool.BUFFER_SIZE);
		this.path = path;
		this.uncompressedBuffers = new ArrayDeque<Buffer>();
		this.compressedBuffers = new ArrayDeque<Buffer>();
	}

	private void swapUncompressedBuffer() {

		if (this.uncompressedBuffer != null) {

			final Buffer buffer = new Buffer(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer);
			this.uncompressedBuffers.push(buffer);
			this.numberOfBytesInUncompressedBuffer = 0;
		}

		this.uncompressedBuffer = this.bufferPool.lockBuffer();
		if (this.uncompressedBuffer == null) {
			System.out.println("Handle uncompressed buffer == null");
		}
	}

	private void swapCompressedBuffer() {

		if (this.compressedBuffer != null) {

			if (this.compressedBuffer.length - this.numberOfBytesInCompressedBuffer >= 4) {
				return;
			}

			final Buffer buffer = new Buffer(this.compressedBuffer, this.numberOfBytesInCompressedBuffer);
			this.compressedBuffers.push(buffer);
			this.numberOfBytesInCompressedBuffer = 0;
		}

		this.compressedBuffer = this.bufferPool.lockBuffer();
		if (this.compressedBuffer == null) {
			System.out.println("Handle compressed buffer == null");
		}
	}

	void write(final InputStream inputStream) throws IOException {

		while (true) {

			// Get a new uncompressed buffer
			swapUncompressedBuffer();

			// Read the complete block
			int r;
			while ((r = inputStream.read(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer,
				this.uncompressedBuffer.length - this.numberOfBytesInUncompressedBuffer)) >= 0) {
				this.numberOfBytesInUncompressedBuffer += r;

				if (this.numberOfBytesInUncompressedBuffer == this.uncompressedBuffer.length) {
					break;
				}
			}

			System.out.println("WRITE " + this.numberOfBytesInUncompressedBuffer);

			// Compress the block
			final int numberOfCompressedBytes = this.compressor.compress(this.uncompressedBuffer,
				this.numberOfBytesInUncompressedBuffer);

			System.out.println("COMPRESSED " + numberOfCompressedBytes);

			swapCompressedBuffer();

			NumberUtils.integerToByteArray(numberOfCompressedBytes, this.compressedBuffer,
				this.numberOfBytesInCompressedBuffer);
			this.numberOfBytesInCompressedBuffer += 4;

			r = 0;
			while (r < numberOfCompressedBytes) {

				final int bytesToCopy = Math.min(numberOfCompressedBytes, this.compressedBuffer.length
					- this.numberOfBytesInCompressedBuffer);
				System.arraycopy(this.compressor.getCompressedBuffer(), r, this.compressedBuffer,
					this.numberOfBytesInCompressedBuffer, bytesToCopy);

				r += bytesToCopy;
				this.numberOfBytesInCompressedBuffer += bytesToCopy;

				swapCompressedBuffer();
			}

			// Check if this was the last buffer
			if (this.numberOfBytesInUncompressedBuffer < uncompressedBuffer.length) {
				break;
			}
		}

		if (this.uncompressedBuffer != null) {
			final Buffer buffer = new Buffer(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer);
			this.uncompressedBuffers.push(buffer);
			this.numberOfBytesInUncompressedBuffer = 0;
			this.uncompressedBuffer = null;
		}

		if (this.compressedBuffer != null) {
			final Buffer buffer = new Buffer(this.compressedBuffer, this.numberOfBytesInCompressedBuffer);
			this.compressedBuffers.push(buffer);
			this.numberOfBytesInCompressedBuffer = 0;
			this.compressedBuffer = null;
		}

		System.out.println("UNCOMPRESSED " + this.uncompressedBuffers.size());
		System.out.println("COMPRESSED " + this.compressedBuffers.size());
	}
}
