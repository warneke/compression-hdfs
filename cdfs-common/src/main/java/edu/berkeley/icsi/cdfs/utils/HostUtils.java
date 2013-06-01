package edu.berkeley.icsi.cdfs.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class HostUtils {

	private HostUtils() {
	}

	public static String determineHostname() {

		try {
			final InetAddress addr = InetAddress.getLocalHost();
			return addr.getHostName();

		} catch (UnknownHostException e) {
		}

		return null;
	}
}
