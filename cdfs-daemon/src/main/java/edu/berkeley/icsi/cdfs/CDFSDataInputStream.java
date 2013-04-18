package edu.berkeley.icsi.cdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

final class CDFSDataInputStream extends FSDataInputStream {

	CDFSDataInputStream(final String hostname, final int port, final Path path) throws IOException {
		super(new SharedMemoryInputStream(hostname, port, path));
	}
}
