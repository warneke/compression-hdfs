package edu.berkeley.icsi.cdfs.cache;

import java.util.concurrent.ArrayBlockingQueue;

public final class BufferPool {

	private static final BufferPool INSTANCE = new BufferPool();

	private static final int NUMBER_OF_BUFFERS = 128;

	public static final int BUFFER_SIZE = 64 * 1024;

	private final ArrayBlockingQueue<byte[]> buffers = new ArrayBlockingQueue<byte[]>(NUMBER_OF_BUFFERS);

	private BufferPool() {

		for (int i = 0; i < NUMBER_OF_BUFFERS; ++i) {
			this.buffers.add(new byte[BUFFER_SIZE]);
		}
	}

	public static BufferPool get() {

		return INSTANCE;
	}

	public byte[] lockBuffer() {

		return this.buffers.poll();
	}

	public void releaseBuffer(final byte[] buffer) {

		this.buffers.add(buffer);
	}
}
