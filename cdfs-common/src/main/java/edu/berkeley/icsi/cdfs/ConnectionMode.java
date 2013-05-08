package edu.berkeley.icsi.cdfs;

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
