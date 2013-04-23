package edu.berkeley.icsi.cdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import edu.berkeley.icsi.cdfs.utils.ReliableDatagramSocket;

final class CDFSDataOutputStream extends FSDataOutputStream {

	CDFSDataOutputStream(final ReliableDatagramSocket socket) throws IOException {
		super(new SharedMemoryOutputStream(socket), null);
	}
}
