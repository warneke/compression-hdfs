package edu.berkeley.icsi.cdfs.datanode;

public enum ConnectionMode {

	READ,
	WRITE;

	public static ConnectionMode toConnectionMode(final byte b) {

		if (b == 0) {
			return READ;
		}

		return WRITE;
	}

	public byte toByte() {

		if (this == ConnectionMode.READ) {
			return 0;
		}

		return 1;
	}
}
