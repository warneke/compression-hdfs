package edu.berkeley.icsi.cdfs.wlgen.datagen;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.berkeley.icsi.cdfs.wlgen.File;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.util.JarFileCreator;

public class DataGenerator {

	static final String FILE_SIZE = "file.size";

	private static final int BLOCK_SIZE = 64 * 1024 * 1024; // 64 MB

	private static final int SUBTASKS_PER_INSTANCE = 2;

	private final InetSocketAddress jobManagerAddress;

	private final String basePath;

	private final java.io.File jarFile;

	public DataGenerator(final InetSocketAddress jobManagerAddress, final String basePath) throws IOException {

		this.jobManagerAddress = jobManagerAddress;
		this.basePath = basePath;

		this.jarFile = java.io.File.createTempFile("datagen", ".jar");
		this.jarFile.deleteOnExit();

		final JarFileCreator jfc = new JarFileCreator(this.jarFile);
		jfc.addClass(DataGeneratorInput.class);
		jfc.addClass(DataGeneratorOutput.class);

		jfc.createJarFile();
	}

	private static JobGraph generateJobGraph(final String basePath, final File inputFile) throws IOException {

		final int numberOfSubtasks = (int) 8;

		final JobGraph jobGraph = new JobGraph("Data generator for file " + inputFile.getName());

		final JobInputVertex inputVertex = new JobInputVertex("Input", jobGraph);
		inputVertex.setInputClass(DataGeneratorInput.class);
		inputVertex.setNumberOfSubtasksPerInstance(SUBTASKS_PER_INSTANCE);
		inputVertex.setNumberOfSubtasks(numberOfSubtasks);

		final JobFileOutputVertex outputVertex = new JobFileOutputVertex("Output", jobGraph);
		outputVertex.setFileOutputClass(DataGeneratorOutput.class);
		outputVertex.setNumberOfSubtasksPerInstance(SUBTASKS_PER_INSTANCE);
		outputVertex.setNumberOfSubtasks(numberOfSubtasks);
		outputVertex.setFilePath(new Path(basePath, inputFile.getName()));
		outputVertex.getConfiguration().setLong(FILE_SIZE, inputFile.getSize());
		System.out.println("Writing data to " + outputVertex.getFilePath());
		
		outputVertex.setVertexToShareInstancesWith(inputVertex);

		try {
			inputVertex.connectTo(outputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION,
				DistributionPattern.POINTWISE);
		} catch (JobGraphDefinitionException e) {
			throw new IOException(e);
		}

		return jobGraph;
	}

	public void generate(final File inputFile) throws IOException {

		final JobGraph jobGraph = generateJobGraph(this.basePath, inputFile);
		jobGraph.addJar(new Path("file://" + this.jarFile.getAbsolutePath()));

		final JobClient jobClient = new JobClient(jobGraph, new Configuration(), this.jobManagerAddress);

		try {
			jobClient.submitJobAndWait();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
