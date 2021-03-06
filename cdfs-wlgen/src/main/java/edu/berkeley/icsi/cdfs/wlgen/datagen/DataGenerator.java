package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.berkeley.icsi.cdfs.traces.File;
import edu.berkeley.icsi.cdfs.wlgen.FixedByteRecord;
import edu.berkeley.icsi.cdfs.wlgen.JarFileCreator;
import edu.berkeley.icsi.cdfs.wlgen.MapReduceJob;

public final class DataGenerator {

	public static final byte[] KEY_ALPHABET = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
		'd', 'e', 'f' };

	static final String FILE_SIZE = "file.size";

	static final String OUTPUT_PATH = "output.path";

	static final String COMPRESSION_FACTOR = "compression.factor";

	private static String JAR_FILE = null;

	public static MapReduceJob generateJob(final String basePath, final File inputFile, final Configuration conf)
			throws IOException {

		if (JAR_FILE == null) {
			JAR_FILE = generateJarFile();
		}

		final Configuration jobConf = new Configuration(conf);
		jobConf.set("mapred.jar", JAR_FILE);
		jobConf.setLong(FILE_SIZE, inputFile.getUncompressedFileSize());
		jobConf.set(OUTPUT_PATH, basePath + java.io.File.separator + inputFile.getName());
		jobConf.setInt(COMPRESSION_FACTOR, inputFile.getCompressionFactor());
		final MapReduceJob job = new MapReduceJob(jobConf, "Data generator for file " + inputFile.getName(), 1);
		job.setInputFormatClass(GeneratorInputFormat.class);
		job.setOutputFormatClass(GeneratorOutputFormat.class);
		job.setNumReduceTasks(0);

		return job;
	}

	private static String generateJarFile() throws IOException {

		final java.io.File jarFile = java.io.File.createTempFile("datagen", ".jar");
		jarFile.deleteOnExit();
		final JarFileCreator jfc = new JarFileCreator(jarFile);
		jfc.addClass(GeneratorInputFormat.class);
		jfc.addClass(GeneratorInputSplit.class);
		jfc.addClass(GeneratorRecordReader.class);
		jfc.addClass(GeneratorRecordWriter.class);
		jfc.addClass(GeneratorOutputFormat.class);
		jfc.addClass(GeneratorOutputCommitter.class);
		jfc.addClass(FixedByteRecord.class);
		jfc.addClass(DataGenerator.class);
		jfc.createJarFile();
		return jarFile.getAbsolutePath();
	}
}
