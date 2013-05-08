package edu.berkeley.icsi.cdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Text;

public class CDFSBlockLocation extends BlockLocation {

	private int index;

	public CDFSBlockLocation(final int index, final String[] names, final String[] hosts, final long offset,
			final long length) {
		super(names, hosts, offset, length);

		this.index = index;
	}

	public CDFSBlockLocation() {
	}

	public int getIndex() {
		return this.index;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder("[");
		sb.append(this.index);
		sb.append(", ");
		sb.append(getOffset());
		sb.append(", ");
		sb.append(getLength());
		sb.append(", (");

		try {
			final String[] hosts = getHosts();
			for (int i = 0; i < hosts.length; ++i) {
				sb.append(hosts[i]);
				if (i < (hosts.length - 1)) {
					sb.append(", ");
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		sb.append(")]");

		return sb.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(this.index);
		out.writeLong(this.getOffset());
		out.writeLong(this.getLength());

		final String[] names = getNames();
		if (names == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeInt(names.length);
			for (int i = 0; i < names.length; ++i) {
				Text.writeString(out, names[i]);
			}
		}

		final String[] hosts = getHosts();
		if (hosts == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeInt(hosts.length);
			for (int i = 0; i < hosts.length; ++i) {
				Text.writeString(out, hosts[i]);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		this.index = in.readInt();
		setOffset(in.readLong());
		setLength(in.readLong());

		if (in.readBoolean()) {
			final int numberOfNames = in.readInt();
			final String names[] = new String[numberOfNames];
			for (int i = 0; i < numberOfNames; ++i) {
				names[i] = Text.readString(in);
			}
			setNames(names);
		}

		if (in.readBoolean()) {
			final int numberOfHosts = in.readInt();
			final String hosts[] = new String[numberOfHosts];
			for (int i = 0; i < numberOfHosts; ++i) {
				hosts[i] = Text.readString(in);
			}
			setHosts(hosts);
		}
	}
}
