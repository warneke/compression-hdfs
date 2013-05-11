package edu.berkeley.icsi.cdfs.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class EvictionList implements Writable {

	private ArrayList<ExtendedBlockKey> list = null;

	public EvictionList() {

	}

	public Iterator<ExtendedBlockKey> iterator() {

		if (this.list == null) {
			return Collections.<ExtendedBlockKey> emptyList().iterator();
		}

		return this.list.iterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		final int numberOfElements = arg0.readInt();
		if (numberOfElements == 0) {
			return;
		}

		this.list = new ArrayList<ExtendedBlockKey>(numberOfElements);
		for (int i = 0; i < numberOfElements; ++i) {
			final String path = arg0.readUTF();
			final int index = arg0.readInt();
			final boolean compressed = arg0.readBoolean();

			this.list.add(new ExtendedBlockKey(path, index, compressed));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		if (this.list == null) {
			arg0.writeInt(0);
			return;
		}

		arg0.writeInt(this.list.size());
		final Iterator<ExtendedBlockKey> it = this.list.iterator();
		while (it.hasNext()) {

			final ExtendedBlockKey ebk = it.next();
			arg0.writeUTF(ebk.getPath());
			arg0.writeInt(ebk.getIndex());
			arg0.writeBoolean(ebk.getCompressed());
		}
	}
}
