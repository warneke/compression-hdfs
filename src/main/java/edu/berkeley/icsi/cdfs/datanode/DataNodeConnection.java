package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class DataNodeConnection extends Thread {

	private final Socket socket;

	DataNodeConnection(final Socket socket) {
		super("DataNodeConnection from " + socket.getRemoteSocketAddress());

		this.socket = socket;
		start();
	}

	private static int readHeader(final InputStream inputStream, final byte[] buffer) throws IOException {

		int totalRead = 0;

		while (true) {

			final int read = inputStream.read(buffer, totalRead, buffer.length - totalRead);
			if (read == -1) {
				break;
			}

			totalRead += read;

			if (buffer[totalRead - 1] == 0) {
				break;
			}
		}

		return totalRead;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// Read header first
		try {
			final InputStream inputStream = this.socket.getInputStream();
			byte[] header = new byte[256];

			final int bytesRead = readHeader(inputStream, header);
			if (bytesRead == 0) {
				throw new IOException("Unexpected end of header");
			}

			// Mode
			if (header[0] == DataNode.WRITE_REQUEST) {
				int pathLength = NumberUtils.byteArrayToInteger(header, 1);
				final Path file = new Path(new String(header, 5, pathLength));

				System.out.println("WRITE " + file);

				byte[] data = new byte[4096];
				int totalRead = 0;
				while (true) {

					final int read = inputStream.read(data);
					if (read == -1) {
						break;
					}

					System.out.println("READ " + read);
					
					totalRead += read;
				}

				System.out.println("TOTAL READ " + totalRead);

			} else {
				System.out.println("READ");
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (this.socket != null) {
				try {
					this.socket.close();
				} catch (IOException ioe) {
				}
			}
		}
	}
}
