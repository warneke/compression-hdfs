package edu.berkeley.icsi.cdfs.statistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class UserStatistics implements AbstractStatistics {

	private String jobID;

	private boolean isMap;

	private int taskID;

	private long startTime;

	private long endTime;

	public UserStatistics(final String jobID, final boolean isMap, final int taskID, final long startTime,
			final long endTime) {
		this.jobID = jobID;
		this.isMap = isMap;
		this.taskID = taskID;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public UserStatistics() {
	}

	public String getJobID() {
		return this.jobID;
	}

	public boolean isMap() {
		return this.isMap;
	}

	public int getTaskID() {
		return this.taskID;
	}

	public long getStartTime() {
		return this.startTime;
	}

	public long getEndTime() {
		return this.endTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		this.jobID = arg0.readUTF();
		this.isMap = arg0.readBoolean();
		this.taskID = arg0.readInt();
		this.startTime = arg0.readLong();
		this.endTime = arg0.readLong();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		arg0.writeUTF(this.jobID);
		arg0.writeBoolean(this.isMap);
		arg0.writeInt(this.taskID);
		arg0.writeLong(this.startTime);
		arg0.writeLong(this.endTime);
	}
}
