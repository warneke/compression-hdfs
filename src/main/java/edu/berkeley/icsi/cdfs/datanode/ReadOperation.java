package edu.berkeley.icsi.cdfs.datanode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Decompressor;

final class ReadOperation {

	private final Path cdfsPath;

	private final Decompressor decompressor;

	private Path hdfsPath;

	private int nextBlockIndex = 0;

	private FileSystem hdfs = null;

	private FSDataInputStream hdfsInputStream = null;

	ReadOperation(final Path cdfsPath) {

		this.cdfsPath = cdfsPath;
		this.hdfsPath = constructHDFSPath();
		this.decompressor = new Decompressor(BufferPool.BUFFER_SIZE);
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

	void read(final OutputStream outputStream) throws IOException {

		final byte[] compressedBuffer = new byte[BufferPool.BUFFER_SIZE];

		final byte[] uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];

		int numberOfBytesInCompressedBuffer = 0;

		while (true) {

			if (!openNextHDFSFile()) {
				break;
			}

			numberOfBytesInCompressedBuffer = 0;

			while (true) {

				int read;

				while (true) {

					read = this.hdfsInputStream.read(compressedBuffer, numberOfBytesInCompressedBuffer,
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
		}

	}

	private boolean openNextHDFSFile() throws IOException {

		if (this.hdfs == null) {
			this.hdfs = this.hdfsPath.getFileSystem(new Configuration());
		}

		if (this.hdfsInputStream != null) {
			this.hdfsInputStream.close();
			++this.nextBlockIndex;
			this.hdfsPath = constructHDFSPath();
		}

		try {
			this.hdfsInputStream = this.hdfs.open(this.hdfsPath);
		} catch (FileNotFoundException e) {
			return false;
		}

		return true;
	}
}
