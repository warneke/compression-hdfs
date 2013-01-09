package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

final class CDFSDataInputStream extends FSDataInputStream {

	CDFSDataInputStream(final InputStream inputStream, final OutputStream outputStream, final Path path) throws IOException {
		super(new SeekableInputStream(inputStream, outputStream, path));
	}
}
