package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

final class CDFSDataInputStream extends FSDataInputStream {

	CDFSDataInputStream(final Socket socket, final Path path) throws IOException {
		super(new SharedMemoryInputStream(socket, path));
	}
}
