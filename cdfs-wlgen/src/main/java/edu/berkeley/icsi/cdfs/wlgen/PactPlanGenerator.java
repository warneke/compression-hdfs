package edu.berkeley.icsi.cdfs.wlgen;

import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactString;

final class PactPlanGenerator {

	static Plan toPactPlan(final String basePath, final MapReduceJob mapReduceJob) {

		System.out.println("Generating plan for job " + mapReduceJob.getJobID());
		final String inputFilePath = basePath + Path.SEPARATOR + mapReduceJob.getInputFile().getName();
		// final String outputFilePath = basePath + Path.SEPARATOR + mapReduceJob.getOutputFile().getName() + "_out";
		final String outputFilePath = "file:///tmp/";//basePath + Path.SEPARATOR + "output";

		final GenericDataSource<FixedByteInputFormat> source = new GenericDataSource<FixedByteInputFormat>(
			FixedByteInputFormat.class, "Input");
		source.setParameter(FixedByteInputFormat.FILE_PARAMETER_KEY, inputFilePath);
		source.setDegreeOfParallelism(mapReduceJob.getNumberOfMapTasks());

		final MapContract mapper = MapContract.builder(MapTask.class)
			.input(source)
			.dataDistribution(new ReduceDataDistribution(mapReduceJob.getDataDistribution()))
			.name("Map")
			.build();
		mapper.setDegreeOfParallelism(mapReduceJob.getNumberOfMapTasks());
		double ioRatio = (double) mapReduceJob.getInputFile().getSize()
			/ (double) mapReduceJob.getSizeOfIntermediateData();
		mapper.getParameters().setFloat(MapTask.INPUT_OUTPUT_RATIO, (float) ioRatio);

		final ReduceContract reducer = ReduceContract.builder(ReduceTask.class, PactString.class, 0)
			.input(mapper)
			.name("Reduce")
			.build();
		reducer.setDegreeOfParallelism(mapReduceJob.getNumberOfReduceTasks());
		ioRatio = (double) mapReduceJob.getSizeOfIntermediateData() / (double) mapReduceJob.getOutputFile().getSize();
		reducer.getParameters().setFloat(ReduceTask.INPUT_OUTPUT_RATIO, (float) ioRatio);
		reducer.getParameters().setLong("initialMemory", 1024 * 1024 * 1024);

		final GenericDataSink out = new GenericDataSink(FixedByteOutputFormat.class, reducer, "Output");
		out.setParameter(FixedByteOutputFormat.FILE_PARAMETER_KEY, outputFilePath);
		out.setDegreeOfParallelism(mapReduceJob.getNumberOfReduceTasks());

		final Plan plan = new Plan(out, mapReduceJob.getJobID());

		return plan;
	}
}
