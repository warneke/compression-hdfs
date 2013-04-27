package edu.berkeley.icsi.cdfs.wlgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

final class DataGenerator {

	public static final byte[] KEY_ALPHABET = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
		'd', 'e', 'f' };

	private static final String FILE_SIZE = "file.size";

	private static final String OUTPUT_PATH = "output.path";

	private final String basePath;

	private static class GeneratorInputFormat extends InputFormat<FixedByteRecord, NullWritable> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public RecordReader<FixedByteRecord, NullWritable> createRecordReader(final InputSplit arg0,
				final TaskAttemptContext arg1) throws IOException, InterruptedException {

			final GeneratorInputSplit gis = (GeneratorInputSplit) arg0;
			return new GeneratorRecordReader(gis.getLength());
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public List<InputSplit> getSplits(final JobContext arg0) throws IOException, InterruptedException {

			final Configuration conf = arg0.getConfiguration();
			final long fileSize = conf.getLong(FILE_SIZE, -1L);

			final List<InputSplit> list = new ArrayList<InputSplit>(1);
			list.add(new GeneratorInputSplit(fileSize));

			return list;
		}
	}

	private static class GeneratorInputSplit extends InputSplit implements Writable {

		private long fileSize;

		private GeneratorInputSplit(final long fileSize) {
			this.fileSize = fileSize;
		}

		private GeneratorInputSplit() {
			this.fileSize = 0L;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public long getLength() throws IOException, InterruptedException {
			return this.fileSize;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void readFields(final DataInput arg0) throws IOException {
			this.fileSize = arg0.readLong();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final DataOutput arg0) throws IOException {
			arg0.writeLong(this.fileSize);
		}
	}

	static class GeneratorRecordReader extends RecordReader<FixedByteRecord, NullWritable> {

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
				data[j] = (byte) KEY_ALPHABET[pos];
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

	private static class GeneratorOutputCommitter extends OutputCommitter {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void abortTask(final TaskAttemptContext arg0) throws IOException {
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void commitTask(final TaskAttemptContext arg0) throws IOException {
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean needsTaskCommit(final TaskAttemptContext arg0) throws IOException {
			return false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void setupJob(final JobContext arg0) throws IOException {
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void setupTask(final TaskAttemptContext arg0) throws IOException {
		}
	}

	private static class GeneratorOutputFormat extends OutputFormat<FixedByteRecord, NullWritable> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void checkOutputSpecs(final JobContext arg0) throws IOException, InterruptedException {
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OutputCommitter getOutputCommitter(final TaskAttemptContext arg0) throws IOException,
				InterruptedException {

			return new GeneratorOutputCommitter();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public RecordWriter<FixedByteRecord, NullWritable> getRecordWriter(final TaskAttemptContext arg0)
				throws IOException, InterruptedException {

			final Configuration conf = arg0.getConfiguration();
			final String outputPath = conf.get(OUTPUT_PATH);

			return new GeneratorRecordWriter(new Path(outputPath), conf);
		}
	}

	private static class GeneratorRecordWriter extends RecordWriter<FixedByteRecord, NullWritable> {

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

	DataGenerator(final String basePath) {
		this.basePath = basePath;
	}

	private static Job generateJob(final String basePath, final File inputFile) throws IOException {

		final Configuration conf = new Configuration();
		conf.set("fs.cdfs.impl", "edu.berkeley.icsi.cdfs.CDFS");
		conf.setLong(FILE_SIZE, inputFile.getSize());
		conf.set(OUTPUT_PATH, basePath + inputFile.getName());
		final Job job = new Job(conf, "Data generator for file " + inputFile.getName());
		job.setInputFormatClass(GeneratorInputFormat.class);
		job.setOutputFormatClass(GeneratorOutputFormat.class);
		job.setNumReduceTasks(0);

		return job;
	}

	public void generate(final File inputFile) throws IOException, InterruptedException, ClassNotFoundException {

		final Job job = generateJob(this.basePath, inputFile);
		job.waitForCompletion(true);
	}
}
