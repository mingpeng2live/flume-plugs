package com.flume.util;

import java.util.*;


/**
 * 简易操作List
 *
 *
 * @author ming.peng
 * @date 2012-12-6
 * @since 2.2.0
 */
public class JsonList<E> implements List<E> {

	protected final List<E> list;
	
	public JsonList() {
		this.list = new ArrayList<E>();
	}

	public JsonList(List<E> list) {
		this.list = list;
	}

	public JsonList(int size) {
		this.list = new ArrayList<E>(size);
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return list.contains(o);
	}

	@Override
	public Iterator<E> iterator() {
		return list.iterator();
	}

	@Override
	public Object[] toArray() {
		return list.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return list.toArray(a);
	}

	@Override
	public boolean add(E e) {
		return list.add(e);
	}
	
	public JsonList<E> addRthis(E e) {
		list.add(e);
		return this;
	}

	@Override
	public boolean remove(Object o) {
		return list.remove(o);
	}
	
	public JsonList<E> removeRthis(Object o) {
		list.remove(o);
		return this;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return list.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		if (c == null || c.size() == 0) {
			return false;
		}
		return list.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		return list.addAll(index, c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return list.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return list.retainAll(c);
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public E get(int index) {
		return list.get(index);
	}

	@Override
	public E set(int index, E element) {
		return list.set(index, element);
	}

	@Override
	public void add(int index, E element) {
		list.add(index, element);
	}

	@Override
	public E remove(int index) {
		return list.remove(index);
	}

	@Override
	public int indexOf(Object o) {
		return list.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return list.lastIndexOf(o);
	}

	@Override
	public ListIterator<E> listIterator() {
		return list.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		return list.listIterator(index);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		return list.subList(fromIndex, toIndex);
	}

	public static <E> JsonList<E> init(E e){
		return new JsonList<E>().addRthis(e);
	}

	public static <E> JsonList<E> initNull(E e){
		return e == null ? null : new JsonList<E>().addRthis(e);
	}

	public static <E> JsonList<E> initEmpty(E e){
		return e == null ? new JsonList<E>() : new JsonList<E>().addRthis(e);
	}

}
