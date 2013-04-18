package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.icsi.cdfs.cache.Buffer;

final class UncompressedCachedReadOp extends AbstractReadOp {

	private final List<Buffer> buffers;

	UncompressedCachedReadOp(final List<Buffer> buffers) {
		this.buffers = buffers;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	void read(final SocketAddress remoteAddress) throws IOException {

		final Iterator<Buffer> it = this.buffers.iterator();
		while (it.hasNext()) {

			// TODO: Fix me
			//final Buffer buffer = it.next();
			//outputStream.write(buffer.getData(), 0, buffer.getLength());
		}

		// TODO: Release buffers
	}

}
