package edu.berkeley.icsi.cdfs.utils;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public final class ReliableDatagramSocket {

	private final DatagramSocket socket;

	public ReliableDatagramSocket(final int port) throws SocketException {
		this.socket = new DatagramSocket(port);
	}

	public ReliableDatagramSocket() throws SocketException {
		this.socket = new DatagramSocket();
	}

	public void receive(final DatagramPacket p) throws IOException {
		this.socket.receive(p);
	}

	public void send(final DatagramPacket p) throws IOException {
		this.socket.send(p);
	}

	public void close() {
		this.socket.close();
	}
}
