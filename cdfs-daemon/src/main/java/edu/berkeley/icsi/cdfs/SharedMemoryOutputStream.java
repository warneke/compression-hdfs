package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import edu.berkeley.icsi.cdfs.sharedmem.SharedMemoryProducer;

final class SharedMemoryOutputStream extends OutputStream {

	private final DatagramSocket socket;

	private final SocketAddress remoteAddress;

	private SharedMemoryProducer smp = null;

	private ByteBuffer sharedMemoryBuffer = null;

	SharedMemoryOutputStream(final DatagramSocket socket) throws IOException {
		this.socket = socket;

		// Receive ack packet to get remote address
		final byte[] buf = new byte[1];
		final DatagramPacket ackPacket = new DatagramPacket(buf, buf.length);
		this.socket.receive(ackPacket);
		this.remoteAddress = ackPacket.getSocketAddress();
		System.out.println("Remote address is " + this.remoteAddress);
	}

	@Override
	public void write(int b) throws IOException {

		System.out.println("Write 1");
	}

	@Override
	public void close() throws IOException {

		// TODO: Write remaining bytes

		System.out.println("Close");
		this.socket.close();
		if (this.smp != null) {
			this.smp.close();
		}
	}

	@Override
	public void flush() {

		System.out.println("Flush");
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
				this.smp.unlockSharedMemory();
				this.sharedMemoryBuffer = null;
			}
		}
	}
}
