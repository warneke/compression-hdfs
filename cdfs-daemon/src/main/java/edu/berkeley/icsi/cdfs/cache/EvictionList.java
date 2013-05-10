package edu.berkeley.icsi.cdfs.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class EvictionList implements Writable {



	public EvictionList() {

	}

	public Iterator<ExtendedBlockKey> iterator() {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

}
