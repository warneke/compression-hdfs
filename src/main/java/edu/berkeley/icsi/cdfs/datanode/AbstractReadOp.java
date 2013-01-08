package edu.berkeley.icsi.cdfs.datanode;

import java.io.IOException;
import java.io.OutputStream;

abstract class AbstractReadOp {

	abstract void read(final OutputStream outputStream) throws IOException;
}
