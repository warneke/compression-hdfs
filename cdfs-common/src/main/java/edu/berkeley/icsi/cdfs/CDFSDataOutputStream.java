package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.fs.FSDataOutputStream;

final class CDFSDataOutputStream extends FSDataOutputStream {

	CDFSDataOutputStream(final Socket socket) throws IOException {
		super(new SharedMemoryOutputStream(socket), null);
	}
}
