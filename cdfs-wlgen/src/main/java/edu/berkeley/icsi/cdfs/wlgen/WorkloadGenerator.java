package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public final class WorkloadGenerator {

	private MapReduceWorkload mapReduceWorkload = null;

	private void loadWorkloadTraces(final String inputDir, final int mapLimit, final int reduceLimit,
			final long filesizeLimit, final int jobLimit) throws IOException {

		this.mapReduceWorkload = MapReduceWorkload.reconstructFromTraces(inputDir, mapLimit, reduceLimit,
			filesizeLimit, jobLimit);
	}

	private void generateInputData(final String basePath) throws IOException {

		final Path path = new Path(basePath + Path.SEPARATOR + "exp");

		final FileSystem fs = path.getFileSystem(new Configuration());

		fs.mkdirs(path);

		if (this.mapReduceWorkload == null) {
			throw new IllegalStateException("Please load the workload traces before generating the input data");
		}

		final DataGenerator dataGenerator = new DataGenerator(basePath);

		final Map<Long, File> inputFiles = this.mapReduceWorkload.getInputFiles();
		final Iterator<File> it = inputFiles.values().iterator();

		while (it.hasNext()) {
			try {
				dataGenerator.generate(it.next());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void runJobs(final String basePath) throws IOException {

		final Map<String, MapReduceJob> mapReduceJobs = this.mapReduceWorkload.getMapReduceJobs();
		final Iterator<MapReduceJob> it = mapReduceJobs.values().iterator();

		while (it.hasNext()) {

			final Job job = MRJobGenerator.toMRJob(basePath, it.next());
			try {
				job.waitForCompletion(true);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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

		if (!cmd.hasOption("i") || !cmd.hasOption("b")) {
			final HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wlgen", options);
			return;
		}

		inputDir = cmd.getOptionValue("i");
		basePath = cmd.getOptionValue("b");

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

		final WorkloadGenerator wlg = new WorkloadGenerator();

		try {
			// Load the workload traces
			wlg.loadWorkloadTraces(inputDir, mapLimit, reduceLimit, filesizeLimit, jobLimit);

			// Generate input data if requested
			if (generateInput) {
				wlg.generateInputData(basePath);
			}

			wlg.runJobs(basePath);

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}