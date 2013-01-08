package edu.berkeley.icsi.cdfs.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

import edu.berkeley.icsi.cdfs.utils.NumberUtils;

public final class Decompressor {

	private final byte[] tmpBuf;

	private int numberOfBytesInTmpBuf = 0;

	private int numberOfConsumedInputBytes = 0;

	private int numberOfCompressedBytes = -1;

	public Decompressor(final int bufferSize) {
		this.tmpBuf = new byte[Snappy.maxCompressedLength(bufferSize)];
	}

	public int decompress(byte[] input, int inputLength, final byte[] output) throws IOException {

		if (this.numberOfCompressedBytes < 0) {

			if (this.numberOfBytesInTmpBuf > 0) {
				final int numberOfRemainingLengthBytes = 4 - this.numberOfBytesInTmpBuf;
				System.arraycopy(input, 0, this.tmpBuf, this.numberOfBytesInTmpBuf, numberOfRemainingLengthBytes);
				this.numberOfCompressedBytes = NumberUtils.byteArrayToInteger(this.tmpBuf, 0);
				this.numberOfConsumedInputBytes = numberOfRemainingLengthBytes;
				this.numberOfBytesInTmpBuf = 0;
			} else {

				final int remainingDataInInput = inputLength - this.numberOfConsumedInputBytes;
				if (remainingDataInInput >= 4) {
					this.numberOfCompressedBytes = NumberUtils.byteArrayToInteger(input,
						this.numberOfConsumedInputBytes);
					this.numberOfConsumedInputBytes += 4;
				} else {
					System.arraycopy(input, this.numberOfConsumedInputBytes, this.tmpBuf, 0, remainingDataInInput);
					this.numberOfBytesInTmpBuf = remainingDataInInput;
					this.numberOfConsumedInputBytes = 0;
					return -1;
				}
			}
		}

		if (this.numberOfBytesInTmpBuf > 0) {
			final int numberOfRemainingCompressedBytes = this.numberOfCompressedBytes - this.numberOfBytesInTmpBuf;
			final int remainingDataInInput = inputLength - this.numberOfConsumedInputBytes;
			if (numberOfRemainingCompressedBytes > remainingDataInInput) {
				throw new IllegalStateException("Not enough data");
			}

			System.arraycopy(input, 0, this.tmpBuf, this.numberOfBytesInTmpBuf, numberOfRemainingCompressedBytes);
			final int numberOfUncompressedBytes = Snappy.uncompress(this.tmpBuf, 0, this.numberOfCompressedBytes,
				output, 0);
			this.numberOfBytesInTmpBuf = 0;
			this.numberOfConsumedInputBytes = numberOfRemainingCompressedBytes;
			this.numberOfCompressedBytes = -1;

			return numberOfUncompressedBytes;

		} else {

			final int remainingDataInInput = inputLength - this.numberOfConsumedInputBytes;
			if (remainingDataInInput >= this.numberOfCompressedBytes) {
				final int numberOfUncompressedBytes = Snappy.uncompress(input, this.numberOfConsumedInputBytes,
					this.numberOfCompressedBytes, output, 0);
				this.numberOfConsumedInputBytes += this.numberOfCompressedBytes;
				this.numberOfCompressedBytes = -1;
				return numberOfUncompressedBytes;
			}

			System.arraycopy(input, this.numberOfConsumedInputBytes, this.tmpBuf, 0, remainingDataInInput);
			this.numberOfBytesInTmpBuf = remainingDataInInput;
			this.numberOfConsumedInputBytes = 0;

			return -1;
		}
	}

}
