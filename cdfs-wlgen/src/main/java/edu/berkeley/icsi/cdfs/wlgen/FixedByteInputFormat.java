package edu.berkeley.icsi.cdfs.wlgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.io.FileInputFormat.InputSplitOpenThread;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.util.PactConfigConstants;

public final class FixedByteInputFormat implements InputFormat<FixedByteRecord, FileInputSplit> {

	/**
	 * The LOG for logging messages in this class.
	 */
	private static final Log LOG = LogFactory.getLog(FixedByteInputFormat.class);

	/**
	 * The timeout (in milliseconds) to wait for a filesystem stream to respond.
	 */
	static final long DEFAULT_OPENING_TIMEOUT;

	static {
		final long to = GlobalConfiguration.getLong(PactConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
			PactConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
		if (to < 0) {
			LOG.error("Invalid timeout value for filesystem stream opening: " + to + ". Using default value of " +
				PactConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
			DEFAULT_OPENING_TIMEOUT = PactConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT;
		} else if (to == 0) {
			DEFAULT_OPENING_TIMEOUT = Long.MAX_VALUE;
		} else {
			DEFAULT_OPENING_TIMEOUT = to;
		}
	}

	/**
	 * The config parameter which defines the input file path.
	 */
	public static final String FILE_PARAMETER_KEY = "pact.input.file.path";

	/**
	 * The config parameter which defines the number of desired splits.
	 */
	private static final String DESIRED_NUMBER_OF_SPLITS_PARAMETER_KEY = "pact.input.file.numsplits";

	/**
	 * The config parameter for the minimal split size.
	 */
	private static final String MINIMAL_SPLIT_SIZE_PARAMETER_KEY = "pact.input.file.minsplitsize";

	/**
	 * The config parameter for the opening timeout in milliseconds.
	 */
	public static final String INPUT_STREAM_OPEN_TIMEOUT_KEY = "pact.input.file.timeout";

	/**
	 * The fraction that the last split may be larger than the others.
	 */
	private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

	/**
	 * The path to the file that contains the input.
	 */
	private Path filePath;

	/**
	 * The input stream reading from the input file.
	 */
	private FSDataInputStream stream;

	/**
	 * The start of the split that this parallel instance must consume.
	 */
	private long splitStart;

	private long splitEnd;

	/**
	 * The desired number of splits, as set by the configure() method.
	 */
	private int numSplits;

	/**
	 * The the minimal split size, set by the configure() method.
	 */
	private long minSplitSize;

	/**
	 * Stream opening timeout.
	 */
	private long openTimeout;

	private final byte[] buf = new byte[100 * 10000];

	private int bytesInBuffer = 0;

	private int bytesReadFromBuffer = 0;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(final Configuration parameters) {

		// get the file path
		final String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath == null) {
			throw new IllegalArgumentException("Configuration file FileInputFormat does not contain the file path.");
		}

		try {
			this.filePath = new Path(filePath);
		} catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: "
				+ rex.getMessage());
		}

		// get the number of splits
		this.numSplits = parameters.getInteger(DESIRED_NUMBER_OF_SPLITS_PARAMETER_KEY, -1);
		if (this.numSplits == 0 || this.numSplits < -1) {
			this.numSplits = -1;
			if (LOG.isWarnEnabled())
				LOG.warn("Ignoring invalid parameter for number of splits: " + this.numSplits);
		}

		// get the minimal split size
		this.minSplitSize = parameters.getLong(MINIMAL_SPLIT_SIZE_PARAMETER_KEY, 1);
		if (this.minSplitSize < 1) {
			this.minSplitSize = 1;
			if (LOG.isWarnEnabled())
				LOG.warn("Ignoring invalid parameter for minimal split size (requires a positive value): "
					+ this.numSplits);
		}

		this.openTimeout = parameters.getLong(INPUT_STREAM_OPEN_TIMEOUT_KEY, DEFAULT_OPENING_TIMEOUT);
		if (this.openTimeout < 0) {
			this.openTimeout = DEFAULT_OPENING_TIMEOUT;
			if (LOG.isWarnEnabled())
				LOG.warn("Ignoring invalid parameter for stream opening timeout (requires a positive value or zero=infinite): "
					+ this.openTimeout);
		} else if (this.openTimeout == 0) {
			this.openTimeout = Long.MAX_VALUE;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BaseStatistics getStatistics(final BaseStatistics cachedStatistics) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

		// take the desired number of splits into account
		minNumSplits = Math.max(minNumSplits, this.numSplits);

		final Path path = this.filePath;
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();
		long totalLength = 0;

		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] dir = fs.listStatus(path);
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir()) {
					files.add(dir[i]);
					totalLength += dir[i].getLen();
				}
			}
		} else {
			files.add(pathFile);
			totalLength += pathFile.getLen();
		}

		final long maxSplitSize = (minNumSplits < 1) ? Long.MAX_VALUE : (totalLength / minNumSplits +
			(totalLength % minNumSplits == 0 ? 0 : 1));

		// now that we have the files, generate the splits
		int splitNum = 0;
		for (final FileStatus file : files) {

			final long len = file.getLen();
			final long blockSize = file.getBlockSize();

			final long minSplitSize;
			if (this.minSplitSize <= blockSize) {
				minSplitSize = this.minSplitSize;
			}
			else {
				if (LOG.isWarnEnabled())
					LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " +
						blockSize + ". Decreasing minimal split size to block size.");
				minSplitSize = blockSize;
			}

			final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
			final long halfSplit = splitSize >>> 1;

			final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

			if (len > 0) {

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
				Arrays.sort(blocks);

				long bytesUnassigned = len;
				long position = 0;

				int blockIndex = 0;

				while (bytesUnassigned > maxBytesForLastSplit) {
					// get the block containing the majority of the data
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					// create a new split
					FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
						blocks[blockIndex].getHosts());
					inputSplits.add(fis);

					// adjust the positions
					position += splitSize;
					bytesUnassigned -= splitSize;
				}

				// assign the last split
				if (bytesUnassigned > 0) {
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
						bytesUnassigned, blocks[blockIndex].getHosts());
					inputSplits.add(fis);
				}
			} else {
				// special case with a file of zero bytes size
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
				String[] hosts;
				if (blocks.length > 0) {
					hosts = blocks[0].getHosts();
				} else {
					hosts = new String[0];
				}
				final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
				inputSplits.add(fis);
			}
		}

		return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
	}

	/**
	 * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file described by the given
	 * offset.
	 * 
	 * @param blocks
	 *        The different blocks of the file. Must be ordered by their offset.
	 * @param offset
	 *        The offset of the position in the file.
	 * @param startIndex
	 *        The earliest index to look at.
	 * @return The index of the block containing the given position.
	 */
	private final int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex)
	{
		// go over all indexes after the startIndex
		for (int i = startIndex; i < blocks.length; i++) {
			long blockStart = blocks[i].getOffset();
			long blockEnd = blockStart + blocks[i].getLength();

			if (offset >= blockStart && offset < blockEnd) {
				// got the block where the split starts
				// check if the next block contains more than this one does
				if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
					return i + 1;
				} else {
					return i;
				}
			}
		}
		throw new IllegalArgumentException("The given offset is not contained in the any block.");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends FileInputSplit> getInputSplitType() {

		return FileInputSplit.class;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open(final FileInputSplit split) throws IOException {

		if (!(split instanceof FileInputSplit)) {
			throw new IllegalArgumentException("File Input Formats can only be used with FileInputSplits.");
		}

		final FileInputSplit fileSplit = (FileInputSplit) split;

		this.splitStart = roundToNextHundred(fileSplit.getStart());
		final long splitLength = roundToNextHundred(fileSplit.getLength());
		this.splitEnd = this.splitStart + splitLength;

		if (LOG.isDebugEnabled())
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + splitLength + "]");

		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
		isot.start();

		try {
			this.stream = isot.waitForCompletion();
		} catch (Throwable t) {
			throw new IOException("Error opening the Input Split " + fileSplit.getPath() +
				" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}

		// get FSDataInputStream
		this.stream.seek(this.splitStart);
		
		// Reset counters
		this.bytesInBuffer = 0;
		this.bytesReadFromBuffer = 0;
	}

	private static long roundToNextHundred(long val) {

		if (val % 100L == 0) {
			return val;
		}

		final long div = val / 100L;

		return (div * 100L) + 100L;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean reachedEnd() throws IOException {

		return (this.splitStart >= this.splitEnd);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean nextRecord(final FixedByteRecord record) throws IOException {

		// Refill buffer
		if (this.bytesInBuffer == 0) {
			final int bytesToRead = Math.min(this.buf.length, (int) (this.splitEnd - this.splitStart));
			if(bytesToRead <= 0) {
				System.out.println("bytesToRead " + bytesToRead);
				return false;
			}
			
			int bytesRead = 0;
			while(bytesRead < bytesToRead) {
				
				final int r = this.stream.read(this.buf, bytesRead, bytesToRead-bytesRead);
				if(r < 0) {
					System.out.println("r < 0 " + this.splitStart + " vs " + this.splitEnd);
					this.splitStart = this.splitEnd;
					return false;
				}
				bytesRead += r;
			}
			
			this.bytesInBuffer = bytesRead;
			this.bytesReadFromBuffer = 0;
		}

		record.set(this.buf, this.bytesReadFromBuffer);
		this.bytesReadFromBuffer += 100;
		this.splitStart += 100L;

		if (this.bytesReadFromBuffer >= this.bytesInBuffer) {
			this.bytesInBuffer = 0;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		if (this.stream != null) {
			// close input stream
			this.stream.close();
		}
	}

}
