package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import edu.berkeley.icsi.cdfs.CDFS;

public class DataNode {

	private final ServerSocket serverSocket;

	public DataNode() throws IOException {
		this.serverSocket = new ServerSocket(CDFS.DATANODE_DATA_PORT);
	}

	void run() throws IOException {

		while (true) {
			final Socket socket = this.serverSocket.accept();
			new Connection(socket);
		}
	}

	public static void main(final String[] args) {

		DataNode dataNode = null;

		try {

			dataNode = new DataNode();
			dataNode.run();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (dataNode != null) {
				dataNode.shutDown();
			}
		}
	}

	void shutDown() {

		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
