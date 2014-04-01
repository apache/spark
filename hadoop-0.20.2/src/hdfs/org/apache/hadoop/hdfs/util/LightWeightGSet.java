/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.util;

import java.io.PrintStream;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A low memory footprint {@link GSet} implementation,
 * which uses an array for storing the elements
 * and linked lists for collision resolution.
 *
 * No rehash will be performed.
 * Therefore, the internal array will never be resized.
 *
 * This class does not support null element.
 *
 * This class is not thread safe.
 *
 * @param <K> Key type for looking up the elements
 * @param <E> Element type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link LinkedElement} interface.
 */
public class LightWeightGSet<K, E extends K> implements GSet<K, E> {
  /**
   * Elements of {@link LightWeightGSet}.
   */
  public static interface LinkedElement {
    /** Set the next element. */
    public void setNext(LinkedElement next);

    /** Get the next element. */
    public LinkedElement getNext();
  }

  public static final Log LOG = LogFactory.getLog(GSet.class);
  static final int MAX_ARRAY_LENGTH = 1 << 30; //prevent int overflow problem
  static final int MIN_ARRAY_LENGTH = 1;

  /**
   * An internal array of entries, which are the rows of the hash table.
   * The size must be a power of two.
   */
  private final LinkedElement[] entries;
  /** A mask for computing the array index from the hash value of an element. */
  private final int hash_mask;
  /** The size of the set (not the entry array). */
  private int size = 0;
  /** Modification version for fail-fast.
   * @see ConcurrentModificationException
   */
  private volatile int modification = 0;

  /**
   * @param recommended_length Recommended size of the internal array.
   */
  public LightWeightGSet(final int recommended_length) {
    final int actual = actualArrayLength(recommended_length);
    LOG.info("recommended=" + recommended_length + ", actual=" + actual);

    entries = new LinkedElement[actual];
    hash_mask = entries.length - 1;
  }

  //compute actual length
  private static int actualArrayLength(int recommended) {
    if (recommended > MAX_ARRAY_LENGTH) {
      return MAX_ARRAY_LENGTH;
    } else if (recommended < MIN_ARRAY_LENGTH) {
      return MIN_ARRAY_LENGTH;
    } else {
      final int a = Integer.highestOneBit(recommended);
      return a == recommended? a: a << 1;
    }
  }

  @Override
  public int size() {
    return size;
  }

  private int getIndex(final K key) {
    return key.hashCode() & hash_mask;
  }

  private E convert(final LinkedElement e){
    @SuppressWarnings("unchecked")
    final E r = (E)e;
    return r;
  }

  @Override
  public E get(final K key) {
    //validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }

    //find element
    final int index = getIndex(key);
    for(LinkedElement e = entries[index]; e != null; e = e.getNext()) {
      if (e.equals(key)) {
        return convert(e);
      }
    }
    //element not found
    return null;
  }

  @Override
  public boolean contains(final K key) {
    return get(key) != null;
  }

  @Override
  public E put(final E element) {
    //validate element
    if (element == null) {
      throw new NullPointerException("Null element is not supported.");
    }
    if (!(element instanceof LinkedElement)) {
      throw new IllegalArgumentException(
          "!(element instanceof LinkedElement), element.getClass()="
          + element.getClass());
    }
    final LinkedElement e = (LinkedElement)element;

    //find index
    final int index = getIndex(element);

    //remove if it already exists
    final E existing = remove(index, element);

    //insert the element to the head of the linked list
    modification++;
    size++;
    e.setNext(entries[index]);
    entries[index] = e;

    return existing;
  }

  /**
   * Remove the element corresponding to the key,
   * given key.hashCode() == index.
   *
   * @return If such element exists, return it.
   *         Otherwise, return null.
   */
  private E remove(final int index, final K key) {
    if (entries[index] == null) {
      return null;
    } else if (entries[index].equals(key)) {
      //remove the head of the linked list
      modification++;
      size--;
      final LinkedElement e = entries[index];
      entries[index] = e.getNext();
      e.setNext(null);
      return convert(e);
    } else {
      //head != null and key is not equal to head
      //search the element
      LinkedElement prev = entries[index];
      for(LinkedElement curr = prev.getNext(); curr != null; ) {
        if (curr.equals(key)) {
          //found the element, remove it
          modification++;
          size--;
          prev.setNext(curr.getNext());
          curr.setNext(null);
          return convert(curr);
        } else {
          prev = curr;
          curr = curr.getNext();
        }
      }
      //element not found
      return null;
    }
  }

  @Override
  public E remove(final K key) {
    //validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }
    return remove(getIndex(key), key);
  }

  @Override
  public Iterator<E> iterator() {
    return new SetIterator();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("(size=").append(size)
     .append(String.format(", %08x", hash_mask))
     .append(", modification=").append(modification)
     .append(", entries.length=").append(entries.length)
     .append(")");
    return b.toString();
  }

  /** Print detailed information of this object. */
  public void printDetails(final PrintStream out) {
    out.print(this + ", entries = [");
    for(int i = 0; i < entries.length; i++) {
      if (entries[i] != null) {
        LinkedElement e = entries[i];
        out.print("\n  " + i + ": " + e);
        for(e = e.getNext(); e != null; e = e.getNext()) {
          out.print(" -> " + e);
        }
      }
    }
    out.println("\n]");
  }

  private class SetIterator implements Iterator<E> {
    /** The starting modification for fail-fast. */
    private final int startModification = modification;
    /** The current index of the entry array. */
    private int index = -1;
    /** The next element to return. */
    private LinkedElement next = nextNonemptyEntry();

    /** Find the next nonempty entry starting at (index + 1). */
    private LinkedElement nextNonemptyEntry() {
      for(index++; index < entries.length && entries[index] == null; index++);
      return index < entries.length? entries[index]: null;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public E next() {
      if (modification != startModification) {
        throw new ConcurrentModificationException("modification=" + modification
            + " != startModification = " + startModification);
      }

      final E e = convert(next);

      //find the next element
      final LinkedElement n = next.getNext();
      next = n != null? n: nextNonemptyEntry();

      return e;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported.");
    }
  }
}
