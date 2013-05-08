package edu.berkeley.icsi.cdfs.conf;

import edu.berkeley.icsi.cdfs.CDFS;

public final class Configuration extends org.apache.hadoop.conf.Configuration {

	public Configuration() {

		set("fs.cdfs.impl", CDFS.class.getName());
		set("mapred.job.tracker", "localhost:54311");
		set("fs.default.name", "cdfs://localhost:8000");
	}
}
