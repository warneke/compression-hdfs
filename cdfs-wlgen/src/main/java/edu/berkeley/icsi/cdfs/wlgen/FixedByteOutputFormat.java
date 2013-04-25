package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public final class FixedByteOutputFormat implements OutputFormat<FixedByteRecord, NullWritable> {

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RecordWriter<FixedByteRecord, NullWritable> getRecordWriter(FileSystem arg0, JobConf arg1, String arg2,
			Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	
}
