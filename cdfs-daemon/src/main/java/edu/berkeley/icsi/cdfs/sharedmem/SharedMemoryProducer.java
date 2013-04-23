package edu.berkeley.icsi.cdfs.sharedmem;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import edu.berkeley.icsi.cdfs.cache.BufferPool;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;
import edu.berkeley.icsi.cdfs.utils.ReliableDatagramSocket;

public final class SharedMemoryProducer {

	private final ReliableDatagramSocket socket;

	private final SocketAddress remoteAddress;

	private DatagramPacket notificationPacket;

	private final DatagramPacket ackPacket;

	private RandomAccessFile memoryMappedFile = null;

	private MappedByteBuffer sharedMemoryBuffer = null;

	private boolean bufferReady;

	public SharedMemoryProducer(final ReliableDatagramSocket socket, final SocketAddress remoteAddress)
			throws IOException {

		this.socket = socket;
		this.remoteAddress = remoteAddress;

		// Prepare acknowledgment packet
		final byte[] ackBuf = new byte[1];
		this.ackPacket = new DatagramPacket(ackBuf, ackBuf.length);

		this.bufferReady = true;
	}

	private ByteBuffer getBuffer() throws IOException {

		if (this.memoryMappedFile == null) {

			final File file = getTmpFile();

			// Create notification buffer;
			final byte[] filenameBuf = file.getAbsolutePath().getBytes();
			final byte[] notificationBuf = new byte[filenameBuf.length + 4];
			System.arraycopy(filenameBuf, 0, notificationBuf, 4, filenameBuf.length);
			this.notificationPacket = new DatagramPacket(notificationBuf, notificationBuf.length);
			this.notificationPacket.setSocketAddress(this.remoteAddress);
			this.memoryMappedFile = new RandomAccessFile(file, "rw");
			final FileChannel fc = this.memoryMappedFile.getChannel();
			this.sharedMemoryBuffer = fc.map(MapMode.READ_WRITE, 0, BufferPool.BUFFER_SIZE);
		}

		return this.sharedMemoryBuffer;
	}

	private static File getTmpFile() throws IOException {

		final File tmpFile = File.createTempFile("cdfs_", "_pipe.dat");
		tmpFile.deleteOnExit();

		return tmpFile;
	}

	public ByteBuffer lockSharedMemory() throws IOException {

		if (!this.bufferReady) {
			this.socket.receive(this.ackPacket);
			this.bufferReady = true;
		}

		final ByteBuffer buf = getBuffer();
		buf.clear();

		return buf;
	}

	public void unlockSharedMemory() throws IOException {

		if (this.sharedMemoryBuffer == null) {
			throw new IllegalStateException("Shared memory buffer has never been locked before");
		}

		this.sharedMemoryBuffer.flip();

		// Update size in notification packet
		byte[] buf = this.notificationPacket.getData();
		NumberUtils.integerToByteArray(this.sharedMemoryBuffer.limit(), buf, 0);

		// Send notification
		this.socket.send(this.notificationPacket);

		this.bufferReady = false;
	}

	public void close() throws IOException {

		// Update size in notification packet
		byte[] buf = this.notificationPacket.getData();
		NumberUtils.integerToByteArray(-1, buf, 0);

		this.socket.send(this.notificationPacket);

		this.socket.close();
		this.memoryMappedFile.close();
	}
}
