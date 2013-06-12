package edu.berkeley.icsi.cdfs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.Writable;

public final class WritableArrayList<E extends Writable> implements List<E>, Writable {

	private final ArrayList<E> list = new ArrayList<E>(2);

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(final DataInput arg0) throws IOException {

		final int numberOfElements = arg0.readInt();
		for (int i = 0; i < numberOfElements; ++i) {

			final String className = arg0.readUTF();
			final Class<? extends E> clazz;
			try {
				clazz = (Class<? extends E>) Class.forName(className);
			} catch (Exception e) {
				throw new IOException(e);
			}

			final E e;
			try {
				e = clazz.newInstance();
			} catch (Exception ex) {
				throw new IOException(ex);
			}

			e.readFields(arg0);
			this.list.add(e);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput arg0) throws IOException {

		arg0.writeInt(this.list.size());
		final Iterator<E> it = this.list.iterator();
		while (it.hasNext()) {

			final E e = it.next();
			arg0.writeUTF(e.getClass().getName());
			e.write(arg0);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {

		return this.list.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {

		return this.list.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(final Object o) {

		return this.list.contains(o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<E> iterator() {

		return this.list.iterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object[] toArray() {

		return this.list.toArray();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> T[] toArray(final T[] a) {

		return this.list.toArray(a);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean add(final E e) {

		return this.list.add(e);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(final Object o) {

		return this.list.remove(o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsAll(final Collection<?> c) {

		return this.list.containsAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addAll(final Collection<? extends E> c) {

		return this.list.addAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addAll(final int index, final Collection<? extends E> c) {

		return this.list.addAll(index, c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAll(final Collection<?> c) {

		return this.list.removeAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean retainAll(final Collection<?> c) {

		return this.list.retainAll(c);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {

		this.list.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public E get(final int index) {

		return this.list.get(index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public E set(final int index, final E element) {

		return this.list.set(index, element);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void add(final int index, final E element) {

		this.list.add(index, element);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public E remove(final int index) {

		return this.list.remove(index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int indexOf(final Object o) {

		return this.list.indexOf(o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int lastIndexOf(final Object o) {

		return this.list.lastIndexOf(o);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ListIterator<E> listIterator() {

		return this.list.listIterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ListIterator<E> listIterator(final int index) {

		return this.list.listIterator(index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<E> subList(final int fromIndex, final int toIndex) {

		return this.list.subList(fromIndex, toIndex);
	}
}
