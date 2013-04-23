package edu.berkeley.icsi.cdfs.sharedmem;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;
import edu.berkeley.icsi.cdfs.utils.ReliableDatagramSocket;

public final class SharedMemoryConsumer {

	private final ReliableDatagramSocket socket;

	private final DatagramPacket notificationPacket;

	private final DatagramPacket ackPacket;

	private final RandomAccessFile memoryMappedFile;

	private final MappedByteBuffer sharedMemoryBuffer;

	private boolean bufferReady = true;

	public SharedMemoryConsumer(final ReliableDatagramSocket socket) throws IOException {
		this.socket = socket;

		// Create notification packet
		final byte[] buf = new byte[256];
		this.notificationPacket = new DatagramPacket(buf, buf.length);

		// Receive information about memory mapped file
		socket.receive(this.notificationPacket);

		final int bufSize = NumberUtils.byteArrayToInteger(buf, 0);
		final String filename = new String(buf, 4, this.notificationPacket.getLength() - 4);

		this.memoryMappedFile = new RandomAccessFile(filename, "r");
		final FileChannel fc = this.memoryMappedFile.getChannel();

		this.sharedMemoryBuffer = fc.map(MapMode.READ_ONLY, 0, BufferPool.BUFFER_SIZE);
		this.sharedMemoryBuffer.limit(bufSize);

		// Create acknowledgment packet
		final byte[] ackBuf = new byte[1];
		this.ackPacket = new DatagramPacket(ackBuf, ackBuf.length);
		this.ackPacket.setSocketAddress(this.notificationPacket.getSocketAddress());
	}

	public ByteBuffer lockSharedMemory() throws IOException {

		if (!this.bufferReady) {
			this.socket.receive(this.notificationPacket);
			final int bufferSize = NumberUtils.byteArrayToInteger(this.notificationPacket.getData(), 0);
			if (bufferSize == -1) {
				return null;
			}
			this.sharedMemoryBuffer.position(0);
			this.sharedMemoryBuffer.limit(bufferSize);
			this.bufferReady = true;
		}

		return this.sharedMemoryBuffer;
	}

	public void unlockSharedMemory() throws IOException {

		this.bufferReady = false;
		this.socket.send(this.ackPacket);
	}

	public void close() throws IOException {

		this.memoryMappedFile.close();
	}
}
