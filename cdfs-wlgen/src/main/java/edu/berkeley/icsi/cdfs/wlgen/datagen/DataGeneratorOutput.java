package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.util.Random;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.types.StringRecord;

public class DataGeneratorOutput extends AbstractFileOutputTask {

	public static final byte[] KEY_ALPHABET = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
		'e', 'f' };

	private final Random rnd = new Random();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		new RecordReader<StringRecord>(this, StringRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		final Configuration conf = getTaskConfiguration();
		final long fileSize = conf.getLong(DataGenerator.FILE_SIZE, -1L);
		if (fileSize == -1L) {
			throw new IllegalStateException("Cannot retrieve file size from configuration");
		}

		final Path outputPath = getFileOutputPath();
		final FileSystem fs = outputPath.getFileSystem();
		final int numberOfSubtasks = getEnvironment().getCurrentNumberOfSubtasks();
		final int indexInSubtaskGroup = getEnvironment().getIndexInSubtaskGroup();

		Path outputFile;
		long numberOfBytesToWrite;
		if (numberOfSubtasks > 1) {

			// We will write into a directory
			fs.mkdirs(outputPath);
			outputFile = new Path(outputPath + Path.SEPARATOR + "path_" + getEnvironment().getIndexInSubtaskGroup());
			final long regularPartSize = ((fileSize / (numberOfSubtasks - 1)) / 100L) * 100L;
			if (indexInSubtaskGroup < (numberOfSubtasks - 1)) {
				numberOfBytesToWrite = regularPartSize;
			} else {
				numberOfBytesToWrite = fileSize - ((numberOfSubtasks - 1) * regularPartSize);
			}
		} else {
			outputFile = outputPath;
			numberOfBytesToWrite = fileSize;
		}

		FSDataOutputStream outputStream = null;
		try {
			outputStream = fs.create(outputFile, true);

			final byte[] data = new byte[100];
			for (int i = 0; i < data.length; ++i) {
				data[i] = (byte) '_';
			}
			data[99] = (byte) '\n';

			long bytesWritten = 0L;
			while (bytesWritten < numberOfBytesToWrite) {

				generateNewKey(data);

				outputStream.write(data);
				bytesWritten += data.length;
			}

		} finally {
			if (outputStream != null) {
				outputStream.close();
			}
		}
	}

	private void generateNewKey(final byte[] data) {

		int i = this.rnd.nextInt();

		for (int j = 0; j < 8; ++j) {
			final int pos = 0x000f & i;
			data[j] = (byte) KEY_ALPHABET[pos];
			i = i >> 4;
		}
	}
}
