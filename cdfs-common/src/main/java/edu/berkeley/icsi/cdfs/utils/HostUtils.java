package edu.berkeley.icsi.cdfs.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HostUtils {

	private static final ConcurrentMap<String, String> HOST_CACHE = new ConcurrentHashMap<String, String>();

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

	public static String stripFQDN(final String fqdn) {

		String strippedHost = HOST_CACHE.get(fqdn);

		if (strippedHost != null) {
			return strippedHost;
		}

		final int pos = fqdn.indexOf('.');
		if (pos < 0) {
			return fqdn;
		}

		strippedHost = fqdn.substring(0, pos);

		HOST_CACHE.put(fqdn, strippedHost);

		return strippedHost;
	}
}
