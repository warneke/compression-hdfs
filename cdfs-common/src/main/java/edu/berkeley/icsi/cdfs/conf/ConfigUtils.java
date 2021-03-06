package edu.berkeley.icsi.cdfs.conf;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class ConfigUtils {
	
	private static final Log LOG = LogFactory.getLog(ConfigUtils.class);

	private static final Options OPTIONS = new Options();
	static {
		OPTIONS.addOption("c", "confDir", true, "Specifies the directory with the configuration files");
	}

	private ConfigUtils() {
	}

	public static Configuration loadConfiguration(final File confDir) throws ConfigurationException {

		if (!confDir.exists()) {
			throw new ConfigurationException("Provided configuration directory " + confDir + " does not exist");
		}

		final String[] files = confDir.list(new FilenameFilter() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public boolean accept(final File dir, final String name) {

				if (name.endsWith(".xml")) {
					return true;
				}

				return false;
			}
		});

		final Configuration conf = new Configuration();

		for (final String file : files) {
			try {
				final Path p = new Path("file://" + confDir + "/" + file);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding " + p);
				}
				conf.addResource(p);
			} catch (Exception e) {
				throw new ConfigurationException(e);
			}
		}

		return conf;
	}

	public static Configuration loadConfiguration(final String[] args) throws ConfigurationException {

		final CommandLineParser parser = new PosixParser();
		final CommandLine cmd;
		try {
			cmd = parser.parse(OPTIONS, args);
		} catch (ParseException e) {
			throw new ConfigurationException(e);
		}

		if (!cmd.hasOption("c")) {
			throw new ConfigurationException(
				"Cannot determine the configuration directory");
		}

		final String confDir = cmd.getOptionValue("c");

		return loadConfiguration(new File(confDir));
	}
}
