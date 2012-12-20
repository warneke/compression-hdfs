package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.datanode.DataNode;
import edu.berkeley.icsi.cdfs.utils.NumberUtils;

final class CDFSDataOutputStream extends FSDataOutputStream {

	static void sendHeader(final Path f, final OutputStream outputStream) throws IOException {

		final byte[] header = new byte[256];
		header[0] = DataNode.WRITE_REQUEST;
		final byte[] path = f.toString().getBytes();
		NumberUtils.integerToByteArray(path.length, header, 1);
		System.arraycopy(path, 0, header, 5, path.length);
		header[5 + path.length] = 0;

		outputStream.write(header, 0, 6 + path.length);
	}

	CDFSDataOutputStream(final OutputStream outputStream) throws IOException {
		super(outputStream, null);
	}

}
