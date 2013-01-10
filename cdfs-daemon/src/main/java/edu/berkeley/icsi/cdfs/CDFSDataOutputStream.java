package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;

final class CDFSDataOutputStream extends FSDataOutputStream {

	CDFSDataOutputStream(final OutputStream outputStream) throws IOException {
		super(outputStream, null);
	}
}
