package edu.berkeley.icsi.cdfs.wlgen;

import org.apache.hadoop.conf.Configuration;

public class ClusterConfigurator {

	private ClusterConfigurator() {
	}

	public static void addClusterConfiguration(final Configuration conf) {

		conf.set("fs.cdfs.impl", "edu.berkeley.icsi.cdfs.CDFS");
		conf.set("mapred.job.tracker", "localhost:54311");
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("hadoop.job.ugi", "warneke");
	}
}
