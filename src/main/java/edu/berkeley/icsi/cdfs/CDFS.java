package edu.berkeley.icsi.cdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Progressable;

import edu.berkeley.icsi.cdfs.datanode.ConnectionMode;
import edu.berkeley.icsi.cdfs.datanode.Header;
import edu.berkeley.icsi.cdfs.protocols.ClientNameNodeProtocol;
import edu.berkeley.icsi.cdfs.utils.PathWrapper;

public class CDFS extends FileSystem {

	public static final int NAMENODE_RPC_PORT = 10000;

	public static final int DATANODE_RPC_PORT = 10001;

	public static final int DATANODE_DATA_PORT = 10002;

	public static final int CLIENT_RPC_PORT = 10003;

	private ClientNameNodeProtocol nameNode;

	private Path workingDir;

	private URI uri;

	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
			throws IOException {

		System.out.println("append");
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite,
			final int bufferSize, final short replication, final long blockSize, final Progressable progress)
			throws IOException {

		// Check if the file already exists
		if (!this.nameNode.create(new PathWrapper(f))) {
			throw new IOException("File " + f + " does already exist");
		}

		final Socket socket = new Socket("localhost", DATANODE_DATA_PORT);
		final OutputStream outputStream = socket.getOutputStream();
		final Header header = new Header(ConnectionMode.WRITE, f);
		header.sendHeader(outputStream);
		return new CDFSDataOutputStream(outputStream);
	}

	@Override
	public boolean delete(Path arg0) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("delete");

		return false;
	}

	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("delete");

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(final Path arg0) throws IOException {

		System.out.println("getFileStatus");

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public URI getUri() {

		return this.uri;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(final URI uri, final Configuration conf)
			throws IOException {
		super.initialize(uri, conf);
		setConf(conf);

		String host = uri.getHost();
		if (host == null) {
			throw new IOException("Incomplete CDFS URI, no host: " + uri);
		}

		final InetSocketAddress nnAddress = NameNode.getAddress(uri
			.getAuthority());
		// this.dfs = new DFSClient(namenode, conf, statistics);
		this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
		this.workingDir = getHomeDirectory();

		this.nameNode = (ClientNameNodeProtocol) RPC.getProxy(ClientNameNodeProtocol.class, 1, new InetSocketAddress(
			"localhost", CDFS.NAMENODE_RPC_PORT), new Configuration());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getWorkingDirectory() {

		return this.workingDir;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("listStatus");

		return null;
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		// TODO Auto-generated method stub

		System.out.println("mkdirs");

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataInputStream open(final Path arg0, int arg1) throws IOException {

		final Socket socket = new Socket("localhost", DATANODE_DATA_PORT);
		final OutputStream outputStream = socket.getOutputStream();
		final Header header = new Header(ConnectionMode.READ, arg0);
		header.sendHeader(outputStream);

		return new CDFSDataInputStream(socket.getInputStream());
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setWorkingDirectory(Path arg0) {
		// TODO Auto-generated method stub

	}
}
