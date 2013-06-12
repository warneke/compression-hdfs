package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public abstract class AbstractStatisticsParser {

	private static final byte MAP_USER_STATISTICS_BYTE = 0;

	private static final byte REDUCE_USER_STATISTICS_BYTE = 1;

	private static final byte READ_STATISTICS_BYTE = 2;

	public static void toOutputStream(final AbstractStatistics statistics, final DataOutputStream outputStream)
			throws IOException {

		outputStream.writeByte(typeToByte(statistics.getClass()));
		statistics.write(outputStream);
	}

	private static byte typeToByte(final Class<? extends AbstractStatistics> clazz) {

		if (MapUserStatistics.class.equals(clazz)) {
			return MAP_USER_STATISTICS_BYTE;
		} else if (ReduceUserStatistics.class.equals(clazz)) {
			return REDUCE_USER_STATISTICS_BYTE;
		} else if (ReadStatistics.class.equals(clazz)) {
			return READ_STATISTICS_BYTE;
		}

		throw new IllegalStateException("Unknown mapping for class " + clazz);
	}

	private static Class<? extends AbstractStatistics> byteToType(final byte b) {

		switch (b) {
		case MAP_USER_STATISTICS_BYTE:
			return MapUserStatistics.class;
		case REDUCE_USER_STATISTICS_BYTE:
			return ReduceUserStatistics.class;
		case READ_STATISTICS_BYTE:
			return ReadStatistics.class;
		default:
			throw new IllegalStateException("Unknown mapping for byte " + b);
		}
	}

	public void parse(final DataInputStream inputStream) throws IOException {

		try {
			while (true) {

				final byte b = inputStream.readByte();
				final Class<? extends AbstractStatistics> clazz = byteToType(b);

				final AbstractStatistics as;
				try {
					as = clazz.newInstance();
				} catch (Exception e) {
					throw new IOException(e);
				}

				as.readFields(inputStream);

				switch (b) {
				case MAP_USER_STATISTICS_BYTE:
					processMapUserStatistics((MapUserStatistics) as);
					break;
				case REDUCE_USER_STATISTICS_BYTE:
					processReduceUserStatistics((ReduceUserStatistics) as);
					break;
				case READ_STATISTICS_BYTE:
					processReadStatistics((ReadStatistics) as);
					break;
				default:
					throw new IllegalStateException("No callback method for " + as);
				}

			}
		} catch (EOFException eof) {
		}
	}

	public abstract void processMapUserStatistics(final MapUserStatistics userStatistics);

	public abstract void processReduceUserStatistics(final ReduceUserStatistics userStatistics);

	public abstract void processReadStatistics(final ReadStatistics readStatistics);
}
