package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.berkeley.icsi.cdfs.wlgen.FixedByteRecord;

public final class GeneratorRecordReader extends RecordReader<FixedByteRecord, NullWritable> {

	private final Random rnd = new Random();

	private final NullWritable value = NullWritable.get();

	private final long numberOfBytesToGenerate;

	private FixedByteRecord key = new FixedByteRecord();

	private long numberOfBytesGenerated = 0L;

	GeneratorRecordReader(final long numberOfBytesToGenerate) {
		this.numberOfBytesToGenerate = numberOfBytesToGenerate;

		// Prepare record
		final byte[] buf = this.key.getData();
		for (int i = 0; i < (FixedByteRecord.LENGTH - 1); ++i) {
			buf[i] = '_';
		}
		buf[FixedByteRecord.LENGTH - 1] = '\n';
		generateNewKey(buf);
	}

	private void generateNewKey(final byte[] data) {

		int i = this.rnd.nextInt();

		for (int j = 0; j < FixedByteRecord.KEY_LENGTH; ++j) {
			final int pos = 0x000f & i;
			data[j] = (byte) DataGenerator.KEY_ALPHABET[pos];
			i = i >> 4;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
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

		final double ratio = (double) this.numberOfBytesGenerated / (double) this.numberOfBytesToGenerate;
		return (float) ratio;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(final InputSplit arg0, final TaskAttemptContext arg1) throws IOException,
			InterruptedException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		generateNewKey(this.key.getData());

		final boolean retVal = (this.numberOfBytesGenerated < this.numberOfBytesToGenerate);
		this.numberOfBytesGenerated += FixedByteRecord.LENGTH;

		return retVal;
	}
}
