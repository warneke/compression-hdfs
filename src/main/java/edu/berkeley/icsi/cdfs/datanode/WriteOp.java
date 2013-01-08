package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.cache.CompressedBufferCache;
import edu.berkeley.icsi.cdfs.cache.UncompressedBufferCache;
import edu.berkeley.icsi.cdfs.compression.Compressor;
import edu.berkeley.icsi.cdfs.protocols.DataNodeNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

final class WriteOp {

	private final DataNodeNameNodeProtocol nameNode;

	private final int blockSize;

	private final BufferPool bufferPool;

	private final Compressor compressor;

	private final Path cdfsPath;

	private Path hdfsPath;

	private FileSystem hdfs = null;

	private FSDataOutputStream hdfsOutputStream = null;

	private boolean cacheUncompressed = true;

	private boolean cacheCompressed = true;

	private byte[] uncompressedBuffer = null;

	private byte[] compressedBuffer = null;

	private int numberOfBytesInUncompressedBuffer = 0;

	private int numberOfBytesInCompressedBuffer = 0;

	private int nextBlockIndex = 0;

	private int bytesWrittenInBlock = 0;

	private final List<Buffer> uncompressedBuffers;

	private final List<Buffer> compressedBuffers;

	WriteOp(final DataNodeNameNodeProtocol nameNode, final Path cdfsPath, final int blockSize) {

		this.nameNode = nameNode;
		this.blockSize = blockSize;
		this.bufferPool = BufferPool.get();
		this.compressor = new Compressor(BufferPool.BUFFER_SIZE);
		this.cdfsPath = cdfsPath;
		this.hdfsPath = constructHDFSPath();
		this.uncompressedBuffers = new ArrayList<Buffer>();
		this.compressedBuffers = new ArrayList<Buffer>();
	}

	private Path constructHDFSPath() {

		final URI cdfsURI = this.cdfsPath.toUri();

		URI uri;
		try {
			uri = new URI("hdfs", cdfsURI.getUserInfo(), cdfsURI.getHost(), 9000, cdfsURI.getPath() + "_"
				+ this.nextBlockIndex, cdfsURI.getQuery(), cdfsURI.getFragment());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

		return new Path(uri);
	}

	private final void writeToHDFS() throws IOException {

		if (this.hdfsOutputStream == null) {

			if (this.hdfs == null) {
				this.hdfs = this.hdfsPath.getFileSystem(new Configuration());
			}

			this.hdfsOutputStream = this.hdfs.create(this.hdfsPath);
		}

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
			writeToHDFS();
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

			if (this.bytesWrittenInBlock == this.blockSize) {

				swapCompressedBuffer();

				if (this.hdfsOutputStream != null) {
					this.hdfsOutputStream.close();
					this.hdfsOutputStream = null;
				}

				synchronized (this.nameNode) {
					this.nameNode.createNewBlock(new PathWrapper(this.cdfsPath), new PathWrapper(this.hdfsPath),
						this.nextBlockIndex, this.bytesWrittenInBlock);
				}

				this.bytesWrittenInBlock = 0;
				++this.nextBlockIndex;
				this.hdfsPath = constructHDFSPath();
			}

			// Check if this was the last buffer
			if (this.numberOfBytesInUncompressedBuffer < uncompressedBuffer.length) {
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

		// Report last block
		synchronized (this.nameNode) {
			this.nameNode.createNewBlock(new PathWrapper(this.cdfsPath), new PathWrapper(this.hdfsPath),
				this.nextBlockIndex, this.bytesWrittenInBlock);
		}

		if (this.hdfsOutputStream != null) {
			this.hdfsOutputStream.close();
			this.hdfsOutputStream = null;
		}

		if (!this.uncompressedBuffers.isEmpty()) {
			UncompressedBufferCache.get().addCachedBlock(this.cdfsPath, this.uncompressedBuffers);
		}

		if (!this.compressedBuffers.isEmpty()) {
			CompressedBufferCache.get().addCachedBlock(this.cdfsPath, this.compressedBuffers);
		}
	}

	private final void clearUncompressedBuffers() {

		final Iterator<Buffer> it = this.uncompressedBuffers.iterator();
		while (it.hasNext()) {
			this.bufferPool.releaseBuffer(it.next().getData());
		}

		this.uncompressedBuffers.clear();
	}

	private final void clearCompressedBuffers() {

		final Iterator<Buffer> it = this.compressedBuffers.iterator();
		while (it.hasNext()) {
			this.bufferPool.releaseBuffer(it.next().getData());
		}

		this.compressedBuffers.clear();
	}
}
