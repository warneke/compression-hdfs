package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.net.DatagramSocket;

import org.apache.hadoop.fs.FSDataOutputStream;

final class CDFSDataOutputStream extends FSDataOutputStream {

	CDFSDataOutputStream(final DatagramSocket socket) throws IOException {
		super(new SharedMemoryOutputStream(socket), null);
	}
}
