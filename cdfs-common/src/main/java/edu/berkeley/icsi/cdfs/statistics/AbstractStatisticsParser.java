package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractStatisticsParser {

	private static final byte USER_STATISTICS_BYTE = 0;

	public static void toOutputStream(final AbstractStatistics statistics, final DataOutputStream outputStream)
			throws IOException {

		outputStream.writeByte(typeToByte(statistics.getClass()));
		statistics.write(outputStream);
	}

	private static byte typeToByte(final Class<? extends AbstractStatistics> clazz) {

		if (UserStatistics.class.equals(clazz)) {
			return USER_STATISTICS_BYTE;
		}

		throw new IllegalStateException("Unknown mapping for class " + clazz);
	}

	private static Class<? extends AbstractStatistics> byteToType(final byte b) {

		if (b == USER_STATISTICS_BYTE) {
			return UserStatistics.class;
		}

		throw new IllegalStateException("Unknown mapping for byte " + b);
	}

	public void parse(final InputStream inputStream) throws IOException {

		final DataInputStream dis = new DataInputStream(inputStream);

		try {
			while (true) {

				final byte b = dis.readByte();
				final Class<? extends AbstractStatistics> clazz = byteToType(b);

				final AbstractStatistics as;
				try {
					as = clazz.newInstance();
				} catch (Exception e) {
					throw new IOException(e);
				}

				as.readFields(dis);

				switch (b) {
				case USER_STATISTICS_BYTE:
					processUserStatistics((UserStatistics) as);
					break;
				default:
					throw new IllegalStateException("No callback method for " + as);
				}

			}
		} catch (EOFException eof) {
		} finally {
			dis.close();
		}
	}

	public abstract void processUserStatistics(final UserStatistics userStatistics);
}
