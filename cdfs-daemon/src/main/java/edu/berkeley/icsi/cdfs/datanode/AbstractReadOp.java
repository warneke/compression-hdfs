package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.net.SocketAddress;

abstract class AbstractReadOp {

	abstract void read(final SocketAddress remoteAddress) throws IOException;
}
