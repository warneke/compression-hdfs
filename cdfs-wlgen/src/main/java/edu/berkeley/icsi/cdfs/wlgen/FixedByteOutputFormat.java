package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.generic.io.OutputFormat;

public final class FixedByteOutputFormat implements OutputFormat<FixedByteRecord> {

	/**
	 * The key under which the name of the target path is stored in the configuration.
	 */
	public static final String FILE_PARAMETER_KEY = "pact.output.file";

	/**
	 * The config parameter for the opening timeout in milliseconds.
	 */
	public static final String OUTPUT_STREAM_OPEN_TIMEOUT_KEY = "pact.output.file.timeout";

	/**
	 * The path of the file to be written.
	 */
	protected Path outputFilePath;

	/**
	 * The stream to which the data is written;
	 */
	protected FSDataOutputStream stream;

	private final byte[] outputBuf = new byte[100*10000];
	
	private int bytesInOutputBuf = 0;
	
	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.recordio.OutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		// get the file parameter
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath == null) {
			throw new IllegalArgumentException("Configuration file FileOutputFormat does not contain the file path.");
		}

		try {
			this.outputFilePath = new Path(filePath);
		} catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: "
				+ rex.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#open()
	 */
	@Override
	public void open(int taskNumber) throws IOException
	{
		// obtain FSDataOutputStream asynchronously, since HDFS client can not handle InterruptedExceptionsPath p =
		// this.path;
		Path p = this.outputFilePath;
		final FileSystem fs = p.getFileSystem();

		// if the output is a directory, suffix the path with the parallel instance index
		if (fs.exists(p) && fs.getFileStatus(p).isDir()) {
			p = p.suffix("/output_" + taskNumber);
		}

		// remove the existing file before creating the output stream
		if (fs.exists(p)) {
			fs.delete(p, false);
		}

		this.stream = fs.create(p, true);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		if(this.bytesInOutputBuf > 0) {
			this.stream.write(this.outputBuf, 0, this.bytesInOutputBuf);
			this.bytesInOutputBuf = 0;
		}
		
		final FSDataOutputStream s = this.stream;
		if (s != null) {
			this.stream = null;
			s.close();
		}
	}

	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureFileFormat(FileDataSink target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 */
	public static abstract class AbstractConfigBuilder<T>
	{
		/**
		 * The configuration into which the parameters will be written.
		 */
		protected final Configuration config;

		// --------------------------------------------------------------------

		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig
		 *        The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration targetConfig) {
			this.config = targetConfig;
		}

		// --------------------------------------------------------------------

		/**
		 * Sets the timeout after which the output format will abort the opening of the output stream,
		 * if the stream has not responded until then.
		 * 
		 * @param timeoutInMillies
		 *        The timeout, in milliseconds, or <code>0</code> for infinite.
		 * @return The builder itself.
		 */
		public T openingTimeout(int timeoutInMillies) {
			this.config.setLong(OUTPUT_STREAM_OPEN_TIMEOUT_KEY, timeoutInMillies);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}

	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder>
	{
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig
		 *        The configuration into which the parameters will be written.
		 */
		protected ConfigBuilder(Configuration targetConfig) {
			super(targetConfig);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final FixedByteRecord record) throws IOException {

		System.arraycopy(record.getBuffer(), 0, this.outputBuf, this.bytesInOutputBuf, FixedByteRecord.LENGTH);
		this.bytesInOutputBuf += FixedByteRecord.LENGTH;
		
		if(this.bytesInOutputBuf == this.outputBuf.length) {
			this.stream.write(this.outputBuf);
			this.bytesInOutputBuf = 0;
		}
	}
}
