package edu.berkeley.icsi.cdfs.wlgen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JarFileCreator {

	/**
	 * The file extension of java classes.
	 */
	private static final String CLASS_EXTENSION = ".class";

	/**
	 * A set of classes which shall be included in the final jar file.
	 */
	private final Set<Class<?>> classSet = new HashSet<Class<?>>();

	/**
	 * The final jar file.
	 */
	private final File outputFile;

	/**
	 * Constructs a new jar file creator.
	 * 
	 * @param outputFile
	 *        the file which shall contain the output data, i.e. the final jar file
	 */
	public JarFileCreator(final File outputFile) {

		this.outputFile = outputFile;
	}

	/**
	 * Adds a {@link Class} object to the set of classes which shall eventually be included in the jar file.
	 * 
	 * @param clazz
	 *        the class to be added to the jar file.
	 */
	public synchronized void addClass(final Class<?> clazz) {

		this.classSet.add(clazz);
	}

	/**
	 * Creates a jar file which contains the previously added class. The content of the jar file is written to
	 * <code>outputFile</code> which has been provided to the constructor. If <code>outputFile</code> already exists, it
	 * is overwritten by this operation.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while writing to the output file
	 */
	public synchronized void createJarFile() throws IOException {

		// Temporary buffer for the stream copy
		final byte[] buf = new byte[128];

		// Check if output file is valid
		if (this.outputFile == null) {
			throw new IOException("Output file is null");
		}

		// If output file already exists, delete it
		if (this.outputFile.exists()) {
			try {
				this.outputFile.delete();
			} catch (SecurityException se) {
				throw new IOException(se);
			}
		}

		final JarOutputStream jos = new JarOutputStream(new FileOutputStream(this.outputFile), new Manifest());
		final Iterator<Class<?>> it = this.classSet.iterator();
		while (it.hasNext()) {

			final Class<?> clazz = it.next();
			final String entry = clazz.getName().replace('.', '/') + CLASS_EXTENSION;

			jos.putNextEntry(new JarEntry(entry));

			InputStream classInputStream = null;
			try {
				classInputStream = clazz.getResourceAsStream(clazz.getSimpleName() + CLASS_EXTENSION);

				int num = classInputStream.read(buf);
				while (num != -1) {
					jos.write(buf, 0, num);
					num = classInputStream.read(buf);
				}

				classInputStream.close();
				jos.closeEntry();
			} finally {
				if(classInputStream != null) {
					classInputStream.close();
				}
			}
		}

		jos.close();
	}
}
