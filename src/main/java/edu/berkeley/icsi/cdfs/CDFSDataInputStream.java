package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;

final class CDFSDataInputStream extends FSDataInputStream {

	CDFSDataInputStream(final InputStream inputStream) throws IOException {
		super(new SeekableInputStream(inputStream));
	}
}
