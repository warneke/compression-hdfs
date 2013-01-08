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

	private final int blockSize;

	private final BufferPool bufferPool;

	private final Compressor compressor;

	private final Path path;

	private boolean cacheUncompressed = true;

	private boolean cacheCompressed = true;

	private byte[] uncompressedBuffer = null;

	private byte[] compressedBuffer = null;

	private int numberOfBytesInUncompressedBuffer = 0;

	private int numberOfBytesInCompressedBuffer = 0;

	private int nextBlockIndex = 0;

	private int bytesWrittenInBlock = 0;

	private final ArrayDeque<Buffer> uncompressedBuffers;

	private final ArrayDeque<Buffer> compressedBuffers;

	WriteOperation(final Path path, final int blockSize) {

		this.blockSize = blockSize;
		this.bufferPool = BufferPool.get();
		this.compressor = new Compressor(BufferPool.BUFFER_SIZE);
		this.path = path;
		this.uncompressedBuffers = new ArrayDeque<Buffer>();
		this.compressedBuffers = new ArrayDeque<Buffer>();
	}

	private void swapUncompressedBuffer() {

		if (this.cacheUncompressed) {

			if (this.uncompressedBuffer != null) {

				final Buffer buffer = new Buffer(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer);
				this.uncompressedBuffers.push(buffer);
				this.numberOfBytesInUncompressedBuffer = 0;
			}

			this.uncompressedBuffer = this.bufferPool.lockBuffer();
			if (this.uncompressedBuffer == null) {
				clearUncompressedBuffers();
				this.uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];
				this.cacheUncompressed = false;
			}
		} else {
			this.numberOfBytesInUncompressedBuffer = 0;
		}
	}

	private void swapCompressedBuffer() {

		if (this.cacheCompressed) {

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

				if (this.cacheUncompressed) {
					clearUncompressedBuffers();
					if (this.numberOfBytesInUncompressedBuffer > 0) {
						final byte[] newBuf = new byte[BufferPool.BUFFER_SIZE];
						System.arraycopy(this.uncompressedBuffer, 0, newBuf, 0, this.numberOfBytesInUncompressedBuffer);
						this.bufferPool.releaseBuffer(this.uncompressedBuffer);
						this.uncompressedBuffer = newBuf;
					}
					this.cacheCompressed = false;
				}

				this.compressedBuffer = this.bufferPool.lockBuffer();
				if (this.compressedBuffer == null) {
					if (this.cacheCompressed) {
						clearCompressedBuffers();
						this.compressedBuffer = new byte[BufferPool.BUFFER_SIZE];
						this.cacheCompressed = false;
					}
				}
			}
		} else {
			this.numberOfBytesInCompressedBuffer = 0;
		}
	}

	void write(final InputStream inputStream) throws IOException {

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
					this.bufferPool.releaseBuffer(this.uncompressedBuffer);
					this.uncompressedBuffer = null;
				}
				break;
			}

			System.out.println("WRITE " + this.numberOfBytesInUncompressedBuffer);

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

				final int bytesToCopy = Math.min(numberOfCompressedBytes, this.compressedBuffer.length
					- this.numberOfBytesInCompressedBuffer);
				System.arraycopy(this.compressor.getCompressedBuffer(), r, this.compressedBuffer,
					this.numberOfBytesInCompressedBuffer, bytesToCopy);

				r += bytesToCopy;
				this.numberOfBytesInCompressedBuffer += bytesToCopy;

				swapCompressedBuffer();
			}

			if(this.bytesWrittenInBlock == this.blockSize) {
				System.out.println("BLOCK SIZE REACHED");
				this.bytesWrittenInBlock = 0;
			}
			
			// Check if this was the last buffer
			if (this.numberOfBytesInUncompressedBuffer < uncompressedBuffer.length) {
				break;
			}
		}

		if (this.uncompressedBuffer != null && this.cacheUncompressed) {
			final Buffer buffer = new Buffer(this.uncompressedBuffer, this.numberOfBytesInUncompressedBuffer);
			this.uncompressedBuffers.push(buffer);
			this.numberOfBytesInUncompressedBuffer = 0;
			this.uncompressedBuffer = null;
		}

		if (this.compressedBuffer != null && this.cacheCompressed) {
			final Buffer buffer = new Buffer(this.compressedBuffer, this.numberOfBytesInCompressedBuffer);
			this.compressedBuffers.push(buffer);
			this.numberOfBytesInCompressedBuffer = 0;
			this.compressedBuffer = null;
		}

		System.out.println("UNCOMPRESSED BUFFERS " + this.uncompressedBuffers.size());
		System.out.println("COMPRESSED BUFFERS " + this.compressedBuffers.size());

		System.out.println("AVAIL " + this.bufferPool.getNumberOfAvailableBuffers());
	}

	private final void clearUncompressedBuffers() {

		while (!this.uncompressedBuffers.isEmpty()) {
			this.bufferPool.releaseBuffer(this.uncompressedBuffers.poll().getData());
		}
	}

	private final void clearCompressedBuffers() {

		while (!this.compressedBuffers.isEmpty()) {
			this.bufferPool.releaseBuffer(this.compressedBuffers.poll().getData());
		}
	}
}
