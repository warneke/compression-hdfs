package edu.berkeley.icsi.cdfs.datanode;

interface ConnectionDispatcher {

	void removeConnection(Connection connection);

	boolean hasActiveConnections();
}
