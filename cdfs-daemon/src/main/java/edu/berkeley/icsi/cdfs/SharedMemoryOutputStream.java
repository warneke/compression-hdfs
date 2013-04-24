package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.berkeley.icsi.cdfs.sharedmem.SharedMemoryProducer;
import edu.berkeley.icsi.cdfs.utils.ReliableDatagramSocket;

final class SharedMemoryOutputStream extends OutputStream {

	private static final Log LOG = LogFactory.getLog(SharedMemoryOutputStream.class);

	private final Socket socket;

	private final SocketAddress remoteAddress;

	private SharedMemoryProducer smp = null;

	private ByteBuffer sharedMemoryBuffer = null;

	private long totalWritten = 0L;

	SharedMemoryOutputStream(final Socket socket) throws IOException {
		this.socket = socket;

		// Receive ack packet to get remote address
		final byte[] buf = new byte[1];
		final DatagramPacket ackPacket = new DatagramPacket(buf, buf.length);
		this.socket.receive(ackPacket);
		this.remoteAddress = ackPacket.getSocketAddress();
	}

	@Override
	public void write(int b) throws IOException {

		throw new UnsupportedOperationException("Write 1");
	}

	@Override
	public void close() throws IOException {

		if (this.sharedMemoryBuffer != null) {
			if (this.sharedMemoryBuffer.position() != 0) {
				this.totalWritten += this.sharedMemoryBuffer.position();
				this.smp.unlockSharedMemory();
			}
		}

		if (this.smp != null) {
			this.smp.close();
		} else {
			this.socket.close();
		}

		LOG.info("Wrote " + this.totalWritten + " bytes to output stream");
	}

	@Override
	public void flush() {

		throw new UnsupportedOperationException("Flush");
	}

	@Override
	public void write(final byte[] b) throws IOException {

		write(b, 0, b.length);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {

		if (this.smp == null) {
			this.smp = new SharedMemoryProducer(this.socket, this.remoteAddress);
		}

		int written = 0;
		while (written < len) {

			if (this.sharedMemoryBuffer == null) {
				this.sharedMemoryBuffer = this.smp.lockSharedMemory();
			}
			final int bytesToWrite = Math.min(this.sharedMemoryBuffer.remaining(), len - written);

			this.sharedMemoryBuffer.put(b, off + written, bytesToWrite);
			written += bytesToWrite;

			if (!this.sharedMemoryBuffer.hasRemaining()) {
				this.totalWritten += this.sharedMemoryBuffer.capacity();
				this.smp.unlockSharedMemory();
				this.sharedMemoryBuffer = null;
			}
		}
	}
}
