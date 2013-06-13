package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public final class MapReduceJob extends Job {

	private final int numMapTasks;

	public MapReduceJob(final Configuration conf, final String jobName, final int numMapTasks) throws IOException {
		super(conf, jobName);

		this.numMapTasks = numMapTasks;
	}

	public int getNumMapTasks() {
		return this.numMapTasks;
	}
}
