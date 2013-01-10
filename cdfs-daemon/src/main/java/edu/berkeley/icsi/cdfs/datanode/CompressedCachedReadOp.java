package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.compression.Decompressor;

final class CompressedCachedReadOp extends AbstractReadOp {

	private final List<Buffer> buffers;

	private final Decompressor decompressor;

	CompressedCachedReadOp(final List<Buffer> buffers) {
		this.buffers = buffers;
		this.decompressor = new Decompressor(BufferPool.BUFFER_SIZE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	void read(final OutputStream outputStream) throws IOException {

		final byte[] uncompressedBuffer = new byte[BufferPool.BUFFER_SIZE];

		final Iterator<Buffer> it = this.buffers.iterator();
		while (it.hasNext()) {

			final Buffer buffer = it.next();
			// Decompress here
			int numberOfUncompressedBytes = this.decompressor.decompress(buffer.getData(), buffer.getLength(),
				uncompressedBuffer);
			while (numberOfUncompressedBytes > 0) {
				
				// Send data back to client
				outputStream.write(uncompressedBuffer, 0, numberOfUncompressedBytes);

				numberOfUncompressedBytes = this.decompressor.decompress(buffer.getData(), buffer.getLength(),
					uncompressedBuffer);
			}
		}

		// TODO: Release buffers
	}

}
