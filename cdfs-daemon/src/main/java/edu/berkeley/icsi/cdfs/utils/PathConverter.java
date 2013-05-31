package edu.berkeley.icsi.cdfs.utils;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

public final class PathConverter {

	private final String scheme;

	private final String host;

	private final int port;

	public PathConverter(final URI baseURI) {

		this.scheme = baseURI.getScheme();
		this.host = baseURI.getHost();
		this.port = baseURI.getPort();

		if (this.scheme == null) {
			throw new IllegalArgumentException("Scheme of base URI must be defined");
		}

		if (this.host == null) {
			throw new IllegalArgumentException("Host of base URI must be defined");
		}

		if (this.port == -1) {
			throw new IllegalArgumentException("Port of base URI must be defined");
		}
	}

	public Path convert(final Path path, final String suffix) {

		final URI oldURI = path.toUri();

		URI uri;
		try {
			uri = new URI(this.scheme, oldURI.getUserInfo(), this.host, this.port, oldURI.getPath() + suffix,
				oldURI.getQuery(), oldURI.getFragment());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

		return new Path(uri);

	}
}
