package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Decompressor;

final class CachingReadOp extends AbstractReadOp {

	private final FileSystem hdfs;

	private final Path hdfsPath;

	private final Decompressor decompressor;

	CachingReadOp(final FileSystem hdfs, final Path hdfsPath) {

		this.hdfs = hdfs;
		this.hdfsPath = hdfsPath;
		this.decompressor = new Decompressor(BufferPool.BUFFER_SIZE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	void read(final OutputStream outputStream) throws IOException {

		final byte[] compressedBuffer = new byte[BufferPool.BUFFER_SIZE];

		final byte[] uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];

		int numberOfBytesInCompressedBuffer = 0;

		FSDataInputStream hdfsInputStream = null;

		try {

			hdfsInputStream = this.hdfs.open(this.hdfsPath);

			while (true) {

				int read;

				while (true) {

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

				// Decompress here
				int numberOfUncompressedBytes = this.decompressor.decompress(compressedBuffer,
					numberOfBytesInCompressedBuffer, uncompressedBuffer);
				while (numberOfUncompressedBytes > 0) {

					// Send data back to client
					outputStream.write(uncompressedBuffer, 0, numberOfUncompressedBytes);

					numberOfUncompressedBytes = this.decompressor.decompress(compressedBuffer,
						numberOfBytesInCompressedBuffer, uncompressedBuffer);
				}

				numberOfBytesInCompressedBuffer = 0;

				if (read < 0) {
					break;
				}
			}
		} finally {

			// Close the input stream
			if (hdfsInputStream != null) {
				hdfsInputStream.close();
			}
		}
	}
}
