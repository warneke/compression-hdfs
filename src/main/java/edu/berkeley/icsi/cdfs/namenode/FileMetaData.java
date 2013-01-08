package edu.berkeley.icsi.cdfs.namenode;

import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final class FileMetaData implements KryoSerializable {

	private Path path;

	FileMetaData(final Path path) {

		this.path = path;
	}

	Path getPath() {
		return this.path;
	}

	@SuppressWarnings("unused")
	private FileMetaData() {
		this.path = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		output.writeString(this.path.toString());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.path = new Path(input.readString());
	}
}
