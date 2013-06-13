package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.berkeley.icsi.cdfs.CDFS;
import edu.berkeley.icsi.cdfs.conf.ConfigConstants;
import edu.berkeley.icsi.cdfs.conf.ConfigUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.berkeley.icsi.cdfs.traces.File;
import edu.berkeley.icsi.cdfs.traces.TraceJob;
import edu.berkeley.icsi.cdfs.traces.TraceWorkload;
import edu.berkeley.icsi.cdfs.wlgen.datagen.DataGenerator;

public final class WorkloadGenerator {

	private final TraceWorkload mapReduceWorkload;

	private WorkloadGenerator(final String inputDir, final int mapLimit, final int reduceLimit,
			final long filesizeLimit, final int jobLimit) throws IOException {

		this.mapReduceWorkload = TraceWorkload.reconstructFromFiles(inputDir, mapLimit, reduceLimit, filesizeLimit,
			jobLimit);
	}

	private void generateInputData(final String basePath, final Configuration conf, final int mapLimit)
			throws ClassNotFoundException, InterruptedException, IOException {

		final Path path = new Path(basePath + Path.SEPARATOR + "exp");

		final FileSystem fs = path.getFileSystem(conf);

		fs.mkdirs(path);

		if (this.mapReduceWorkload == null) {
			throw new IllegalStateException("Please load the workload traces before generating the input data");
		}

		final Set<File> inputFiles = this.mapReduceWorkload.getInputFiles();
		final List<MapReduceJob> jobsToExecute = new ArrayList<MapReduceJob>(inputFiles.size());
		final Iterator<File> it = inputFiles.iterator();

		while (it.hasNext()) {
			final MapReduceJob job = DataGenerator.generateJob(basePath, it.next(), conf);
			jobsToExecute.add(job);
		}

		RemoteJobRunner.submitAndWait(jobsToExecute, mapLimit);
	}

	private void runJobs(final String basePath, final Configuration conf, final int mapLimit)
			throws ClassNotFoundException, InterruptedException, IOException {

		final Map<String, TraceJob> mapReduceJobs = this.mapReduceWorkload.getMapReduceJobs();
		final List<MapReduceJob> jobsToExecute = new ArrayList<MapReduceJob>(mapReduceJobs.size());
		final Iterator<TraceJob> it = mapReduceJobs.values().iterator();

		while (it.hasNext()) {
			final MapReduceJob job = MRJobGenerator.toMRJob(basePath, it.next(), conf);
			jobsToExecute.add(job);
		}

		RemoteJobRunner.submitAndWait(jobsToExecute, mapLimit);
	}

	public static void main(final String[] args) {

		final Options options = new Options();
		options.addOption("i", "input", true, "Specifies the input directory containing the traces");
		options.addOption("b", "base", true, "The base path for the input and output data");
		options.addOption("g", "generate", false, "Generate the input files before running the jobs");
		options.addOption("m", "map", true, "Only run jobs with less than the specified number of map tasks");
		options.addOption("r", "reduce", true, "Only run jobs with less than the specified number of reduce tasks");
		options.addOption("f", "filesize", true,
			"Only run jobs whose input file size is less than the specified value in bytes");
		options.addOption("l", "limit", true, "Limit the number of jobs to run to the specified value");
		options.addOption("c", "config", true, "The location of the configuration directory");

		String inputDir = null;
		String basePath = null;
		String confDir = null;
		boolean generateInput = false;
		int mapLimit = Integer.MAX_VALUE;
		int reduceLimit = Integer.MAX_VALUE;
		long filesizeLimit = Long.MAX_VALUE;
		int jobLimit = Integer.MAX_VALUE;

		final CommandLineParser parser = new PosixParser();
		final CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}

		if (!cmd.hasOption("i") || !cmd.hasOption("b") || !cmd.hasOption("c")) {
			final HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wlgen", options);
			return;
		}

		inputDir = cmd.getOptionValue("i");
		basePath = cmd.getOptionValue("b");
		confDir = cmd.getOptionValue("c");

		final Configuration conf;
		try {
			conf = ConfigUtils.loadConfiguration(new java.io.File(confDir));
		} catch (ConfigurationException e) {
			e.printStackTrace();
			return;
		}

		// Set some basic configuration options
		conf.set("fs.cdfs.impl", CDFS.class.getName());
		conf.set("fs.default.name",
			conf.get(ConfigConstants.CDFS_DEFAULT_NAME_KEY, ConfigConstants.DEFEAULT_CDFS_DEFAULT_NAME));

		if (cmd.hasOption("g")) {
			generateInput = true;
		}

		if (cmd.hasOption("m")) {
			mapLimit = Integer.parseInt(cmd.getOptionValue("m"));
		}

		if (cmd.hasOption("r")) {
			reduceLimit = Integer.parseInt(cmd.getOptionValue("r"));
		}

		if (cmd.hasOption("f")) {
			filesizeLimit = Long.parseLong(cmd.getOptionValue("f"));
		}

		if (cmd.hasOption("l")) {
			jobLimit = Integer.parseInt(cmd.getOptionValue("l"));
		}

		try {
			final WorkloadGenerator wlg = new WorkloadGenerator(inputDir, mapLimit, reduceLimit, filesizeLimit,
				jobLimit);

			// Generate input data if requested
			if (generateInput) {
				wlg.generateInputData(basePath, conf, mapLimit);
			} else {
				wlg.runJobs(basePath, conf, mapLimit);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
