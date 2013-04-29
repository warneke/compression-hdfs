package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.berkeley.icsi.cdfs.wlgen.FixedByteRecord;

public final class GeneratorRecordWriter extends RecordWriter<FixedByteRecord, NullWritable> {

	private final FileSystem fs;

	private final FSDataOutputStream outputStream;

	private final byte[] buffer = new byte[4000];

	private int numberOfBytesInBuffer = 0;

	GeneratorRecordWriter(final Path path, final Configuration conf) throws IOException {

		this.fs = path.getFileSystem(conf);
		this.outputStream = this.fs.create(path, true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close(final TaskAttemptContext arg0) throws IOException, InterruptedException {

		if (this.numberOfBytesInBuffer > 0) {
			this.outputStream.write(this.buffer, 0, this.numberOfBytesInBuffer);
			this.numberOfBytesInBuffer = 0;
		}

		this.outputStream.close();
		this.fs.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final FixedByteRecord arg0, final NullWritable arg1) throws IOException, InterruptedException {

		if (this.numberOfBytesInBuffer + FixedByteRecord.LENGTH > this.buffer.length) {
			this.outputStream.write(this.buffer, 0, this.numberOfBytesInBuffer);
			this.numberOfBytesInBuffer = 0;
		}

		System.arraycopy(arg0.getData(), 0, this.buffer, this.numberOfBytesInBuffer, FixedByteRecord.LENGTH);
		this.numberOfBytesInBuffer += FixedByteRecord.LENGTH;
	}
}