package edu.berkeley.icsi.cdfs.wlgen;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.berkeley.icsi.cdfs.wlgen.datagen.DataGenerator;

public final class ReducePartitioner extends Partitioner<FixedByteRecord, NullWritable> implements Configurable {

	private static final Log LOG = LogFactory.getLog(ReducePartitioner.class);

	private FixedByteRecord[] boundaries;

	private Configuration conf = null;

	public static final String DATA_DISTRIBUTION = "data.distribution";

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPartition(final FixedByteRecord arg0, final NullWritable arg1, final int arg2) {

		// bin search the bucket
		int low = 0, high = this.boundaries.length - 1;

		while (low <= high) {
			final int mid = (low + high) >>> 1;
			final int result = arg0.compareTo(this.boundaries[mid]);

			if (result < 0) {
				high = mid - 1;
			} else if (result > 0) {
				low = mid + 1;
			} else {
				return mid % arg2;
			}
		}

		return low % arg2; // key not found, but the low index is the target
		// bucket, since the boundaries are the upper bound
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setConf(final Configuration configuration) {

		this.conf = configuration;

		// Decode data distribution
		final String distString = configuration.get(DATA_DISTRIBUTION);
		if (distString == null) {
			throw new IllegalStateException("Cannot determine data distribution");
		}

		final double[] dataDistribution = decodeDataDistribution(distString);

		this.boundaries = new FixedByteRecord[dataDistribution.length];
		for (int i = 0; i < dataDistribution.length; ++i) {
			this.boundaries[i] = toBucketBoundary(dataDistribution[i]);
		}

		// Write boundaries to log
		if (LOG.isInfoEnabled()) {
			final StringBuilder sb = new StringBuilder("REDUCE PARTITIONER\n");
			for (int i = 0; i < dataDistribution.length; ++i) {
				sb.append(i);
				sb.append(": ");
				sb.append(this.boundaries[i]);
			}

			LOG.info(sb.toString());
		}
	}

	private static FixedByteRecord toBucketBoundary(final double val) {

		final FixedByteRecord record = new FixedByteRecord();

		for (int j = 0; j < FixedByteRecord.LENGTH; ++j) {
			record.getData()[j] = '_';
		}
		record.getData()[FixedByteRecord.LENGTH - 1] = '\n';

		long l = (((long) (val * (double) Integer.MAX_VALUE * 2)) & 0xFFFFFFFF);

		for (int j = 0; j < 8; ++j) {
			final int pos = (int) (0x000f & l);
			record.getData()[j] = DataGenerator.KEY_ALPHABET[pos];
			l = l >> 4;
		}

		reverseKey(record.getData());

		return record;
	}

	private static void reverseKey(final byte[] buf) {

		swap(buf, 0, 7);
		swap(buf, 1, 6);
		swap(buf, 2, 5);
		swap(buf, 3, 4);
	}

	private static void swap(final byte[] buf, final int pos1, final int pos2) {

		final byte tmp = buf[pos1];
		buf[pos1] = buf[pos2];
		buf[pos2] = tmp;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getConf() {
		return this.conf;
	}

	private static double[] decodeDataDistribution(final String distString) {

		final ByteArrayInputStream bais = new ByteArrayInputStream(distString.getBytes());
		final Base64InputStream b64is = new Base64InputStream(bais);
		final DataInputStream dis = new DataInputStream(b64is);

		try {
			final int numberOfBins = dis.readInt();
			final double[] dist = new double[numberOfBins];
			for (int i = 0; i < numberOfBins; ++i) {
				dist[i] = dis.readDouble();
			}
			dis.close();

			return dist;

		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

	public static String encodeDataDistribution(final double[] distribution) {

		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final Base64OutputStream b64os = new Base64OutputStream(baos);
			final DataOutputStream dos = new DataOutputStream(b64os);

			dos.writeInt(distribution.length);
			for (int i = 0; i < distribution.length; ++i) {
				dos.writeDouble(distribution[i]);
			}
			dos.close();

			return new String(baos.toByteArray());

		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
}
