package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.net.InetSocketAddress;
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
			dataGenerator.generate(it.next());
		}
	}

	private void runJobs(final InetSocketAddress jobManagerAddress, final String basePath) throws IOException {

		final Map<String, MapReduceJob> mapReduceJobs = this.mapReduceWorkload.getMapReduceJobs();
		final Iterator<MapReduceJob> it = mapReduceJobs.values().iterator();

		// final MultiJobClient mjc = new MultiJobClient(jobManagerAddress);

		final java.io.File jarFile = java.io.File.createTempFile("job_", ".jar");
		jarFile.deleteOnExit();

		final JarFileCreator jfc = new JarFileCreator(jarFile);
		jfc.addClass(MapTask.class);
		jfc.addClass(ReduceTask.class);
		jfc.addClass(ReduceDataDistribution.class);
		jfc.addClass(IORatioAdapter.class);
		jfc.addClass(FixedByteInputFormat.class);
		jfc.addClass(FixedByteOutputFormat.class);
		jfc.addClass(FixedByteOutputFormat.class);
		jfc.addClass(FixedByteRecord.class);
		jfc.addClass(FixedByteRecordFactory.class);
		jfc.addClass(FixedByteRecordSerializer.class);
		jfc.addClass(FixedByteRecordComparator.class);
		jfc.createJarFile();

		final Path jarFilePath = new Path("file:///" + jarFile.getAbsolutePath());

		while (it.hasNext()) {

			final Plan plan = PactPlanGenerator.toPactPlan(basePath, it.next());
			final OptimizedPlan optimizedPlan = this.pactCompiler.compile(plan);
			final JobGraphGenerator jobGraphGenerator = new JobGraphGenerator();
			final JobGraph jobGraph = jobGraphGenerator.compileJobGraph(optimizedPlan);
			final JobClient jobClient = new JobClient(jobGraph, new Configuration(), jobManagerAddress);

			jobGraph.addJar(jarFilePath);

			try {
				jobClient.submitJobAndWait();
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}

			jobClient.close();
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
		String configDir = null;
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
		configDir = cmd.getOptionValue("c");

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
				wlg.generateInputData(jobManagerAddress, basePath);
			}

			wlg.runJobs(jobManagerAddress, basePath);

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
