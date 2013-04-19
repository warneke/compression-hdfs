package edu.berkeley.icsi.cdfs.datanode;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.icsi.cdfs.cache.Buffer;
import edu.berkeley.icsi.cdfs.sharedmem.SharedMemoryProducer;

final class ReadOp implements Closeable {

	private final SharedMemoryProducer sharedMemoryProducer;

	ReadOp(final DatagramSocket socket, final SocketAddress remoteAddress) throws IOException {
		this.sharedMemoryProducer = new SharedMemoryProducer(socket, remoteAddress);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.sharedMemoryProducer.close();
	}
}
