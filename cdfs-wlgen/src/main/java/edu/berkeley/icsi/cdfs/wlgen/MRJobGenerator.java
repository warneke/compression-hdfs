package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import edu.berkeley.icsi.cdfs.traces.TraceJob;
import edu.berkeley.icsi.cdfs.wlgen.datagen.DataGenerator;

final class MRJobGenerator {

	private static String JAR_FILE = null;

	private static String generateJarFile() throws IOException {

		final java.io.File jarFile = java.io.File.createTempFile("datagen", ".jar");
		jarFile.deleteOnExit();
		final JarFileCreator jfc = new JarFileCreator(jarFile);
		jfc.addClass(DataGenerator.class);
		jfc.addClass(FixedByteInputFormat.class);
		jfc.addClass(FixedByteInputSplit.class);
		jfc.addClass(FixedByteRecordReader.class);
		jfc.addClass(FixedByteRecordWriter.class);
		jfc.addClass(FixedByteOutputFormat.class);
		jfc.addClass(FixedByteOutputCommitter.class);
		jfc.addClass(FixedByteRecord.class);
		jfc.addClass(MapTask.class);
		jfc.addClass(ReduceTask.class);
		jfc.addClass(StatisticsCollector.class);
		jfc.addClass(IORatioAdapter.class);
		jfc.createJarFile();
		return jarFile.getAbsolutePath();
	}

	static MapReduceJob toMRJob(final String basePath, final TraceJob traceJob, final Configuration conf)
			throws IOException {

		if (JAR_FILE == null) {
			JAR_FILE = generateJarFile();
		}

		final Configuration jobConf = new Configuration(conf);
		jobConf.set(StatisticsCollector.JOB_NAME_CONF_KEY, traceJob.getJobID());
		jobConf.set("mapred.jar", JAR_FILE);
		jobConf.set(FixedByteInputFormat.INPUT_PATH, basePath + java.io.File.separator
			+ traceJob.getInputFile().getName());
		jobConf.setInt(FixedByteInputFormat.NUMBER_OF_MAPPERS, traceJob.getNumberOfMapTasks());
		double ioRatio = (double) traceJob.getInputFile().getUncompressedFileSize()
			/ (double) traceJob.getSizeOfIntermediateData();
		jobConf.setFloat(MapTask.INPUT_OUTPUT_RATIO, (float) ioRatio);
		ioRatio = (double) traceJob.getSizeOfIntermediateData()
			/ (double) traceJob.getOutputFile().getUncompressedFileSize();
		jobConf.setFloat(ReduceTask.INPUT_OUTPUT_RATIO, (float) ioRatio);
		jobConf.set(FixedByteOutputFormat.OUTPUT_PATH, basePath + java.io.File.separator
			+ traceJob.getOutputFile().getName() + "_out");

		final MapReduceJob job = new MapReduceJob(jobConf, traceJob.getJobID(), traceJob.getNumberOfMapTasks(),
			traceJob);
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setNumReduceTasks(traceJob.getNumberOfReduceTasks());
		job.setInputFormatClass(FixedByteInputFormat.class);
		job.setOutputFormatClass(FixedByteOutputFormat.class);
		job.setMapOutputKeyClass(FixedByteRecord.class);
		job.setMapOutputValueClass(NullWritable.class);

		return job;
	}
}
