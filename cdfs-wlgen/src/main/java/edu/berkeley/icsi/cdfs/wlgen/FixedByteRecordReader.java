package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public final class FixedByteRecordReader extends RecordReader<FixedByteRecord, NullWritable> {

	private static final Log LOG = LogFactory.getLog(FixedByteRecordReader.class);

	private final FileSystem fileSystem;

	private final FSDataInputStream inputStream;

	private final FixedByteRecord key = new FixedByteRecord();

	private final NullWritable value = NullWritable.get();

	private final long numberOfBytesToRead;

	private final StatisticsCollector statisticsCollector;

	private long numberOfBytesRead = 0;

	public FixedByteRecordReader(final FixedByteInputSplit inputSplit, final Configuration conf, final int taskID,
			final boolean isLast) throws IOException, InterruptedException {

		this.statisticsCollector = StatisticsCollector
			.forMap(conf, taskID, inputSplit.getPath(), inputSplit.getIndex());

		this.fileSystem = inputSplit.getPath().getFileSystem(conf);
		this.inputStream = this.fileSystem.open(inputSplit.getPath());
		this.inputStream.seek(inputSplit.getOffset());

		final long roundedOffset = roundToNextMultipleOf(inputSplit.getOffset(), FixedByteRecord.LENGTH);
		final long offsetDiff = roundedOffset - inputSplit.getOffset();

		final long roundedLength;
		if (isLast) {
			roundedLength = inputSplit.getLength() - offsetDiff;
		} else {
			roundedLength = roundToNextMultipleOf(inputSplit.getLength(), FixedByteRecord.LENGTH);
		}

		if (offsetDiff != 0L) {
			consumeOffset((int) offsetDiff);
		}

		this.numberOfBytesToRead = roundedLength;

		LOG.info("Starting to read " + inputSplit.getPath() + " at byte " + roundedOffset + " length " + roundedLength);
	}

	private final void consumeOffset(final int offset) throws IOException {

		final byte[] buf = new byte[offset];

		int bytesRead = 0;

		while (bytesRead < offset) {

			final int r = this.inputStream.read(buf, bytesRead, offset - bytesRead);
			if (r < 0) {
				throw new IllegalStateException("Unexpected end of stream");
			}
			bytesRead += r;
		}
	}

	private static long roundToNextMultipleOf(final long val, final long multiple) {

		if (val == 0L) {
			return 0L;
		}

		return (((val - 1L) / multiple) + 1L) * multiple;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.inputStream.close();
		this.fileSystem.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FixedByteRecord getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {

		final double ratio = (double) this.numberOfBytesRead / (double) this.numberOfBytesToRead;
		return (float) ratio;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(final InputSplit arg0, final TaskAttemptContext arg1) throws IOException,
			InterruptedException {

		// Nothing to do here
	}

	private void readNextRecord() throws IOException {

		final byte[] buf = this.key.getData();
		final int bufLength = buf.length;
		int read = 0;
		while (read < bufLength) {
			final int r = this.inputStream.read(buf, read, bufLength - read);
			if (r < 0) {
				throw new IOException("Unexpected end of stream");
			}
			read += r;
		}

		// Verify record
		if (buf[FixedByteRecord.LENGTH - 1] != '\n') {
			throw new IllegalStateException("Illegal record " + this.key);
		}

		this.numberOfBytesRead += bufLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (this.numberOfBytesRead >= this.numberOfBytesToRead) {
			this.statisticsCollector.close();
			return false;
		}

		readNextRecord();

		return true;
	}
}
