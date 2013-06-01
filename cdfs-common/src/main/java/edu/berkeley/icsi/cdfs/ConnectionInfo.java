package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public final class ConnectionInfo implements Writable {

	private String hostname;

	private int port;

	public ConnectionInfo(final String hostname, final int port) {

		if (hostname == null) {
			throw new IllegalArgumentException("Argument hostname must not be null");
		}

		if (port <= 0) {
			throw new IllegalArgumentException("Argument post must be greather than 0");
		}

		this.hostname = hostname;
		this.port = port;
	}

	public ConnectionInfo() {
	}

	public String getHostname() {
		return this.hostname;
	}

	public int getPort() {
		return this.port;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		this.hostname = arg0.readUTF();
		this.port = arg0.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		arg0.writeUTF(this.hostname);
		arg0.writeInt(this.port);
	}

}
