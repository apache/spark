/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection;

import java.util.Comparator;

/**
 * A port of the OpenJDK 6 Arrays.sort(Object[]) function, which utilizes a simple merge sort.
 * This has been kept in Java with the original style in order to match very closely with the JDK
 * source code, and thus be easy to verify correctness.
 *
 * The purpose of the port is to generalize the interface to the sort to accept input data formats
 * besides simple arrays where every element is sorted individually. For instance, the AppendOnlyMap
 * uses this to sort an Array with alternating elements of the form [key, value, key, value].
 * This generalization comes with minimal overhead -- see SortDataFormat for more information.
 */
class Sorter<K, Buffer> {

  private final SortDataFormat<K, Buffer> s;

  public Sorter(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
  }
  /**
   * Tuning parameter: list size at or below which insertion sort will be
   * used in preference to mergesort or quicksort.
   */
  private static final int INSERTIONSORT_THRESHOLD = 7;

  /**
   * Sorts the specified range of the specified array of objects according
   * to the order induced by the specified comparator.  The range to be
   * sorted extends from index <tt>fromIndex</tt>, inclusive, to index
   * <tt>toIndex</tt>, exclusive.  (If <tt>fromIndex==toIndex</tt>, the
   * range to be sorted is empty.)  All elements in the range must be
   * <i>mutually comparable</i> by the specified comparator (that is,
   * <tt>c.compare(e1, e2)</tt> must not throw a <tt>ClassCastException</tt>
   * for any elements <tt>e1</tt> and <tt>e2</tt> in the range).<p>
   *
   * This sort is guaranteed to be <i>stable</i>:  equal elements will
   * not be reordered as a result of the sort.<p>
   *
   * The sorting algorithm is a modified mergesort (in which the merge is
   * omitted if the highest element in the low sublist is less than the
   * lowest element in the high sublist).  This algorithm offers guaranteed
   * n*log(n) performance.
   *
   * @param a the array to be sorted
   * @param fromIndex the index of the first element (inclusive) to be
   *        sorted
   * @param toIndex the index of the last element (exclusive) to be sorted
   * @param c the comparator to determine the order of the array.
   * @throws ClassCastException if the array contains elements that are not
   *         <i>mutually comparable</i> using the specified comparator.
   * @throws IllegalArgumentException if <tt>fromIndex &gt; toIndex</tt>
   * @throws ArrayIndexOutOfBoundsException if <tt>fromIndex &lt; 0</tt> or
   *         <tt>toIndex &gt; a.length</tt>
   */
  public void sort(Buffer a, int fromIndex, int toIndex, Comparator<K> c) {
    int length = toIndex - fromIndex;
    Buffer aux = s.allocate(length);
    s.copyRange(a, fromIndex, aux, 0, length);
    mergeSort(aux, a, fromIndex, toIndex, -fromIndex, c);
  }

  /**
   * Src is the source array that starts at index 0
   * Dest is the (possibly larger) array destination with a possible offset
   * low is the index in dest to start sorting
   * high is the end index in dest to end sorting
   * off is the offset into src corresponding to low in dest
   */
  private void mergeSort(Buffer src, Buffer dest, int low, int high, int off, Comparator<K> c) {
    int length = high - low;

    // Insertion sort on smallest arrays
    if (length < INSERTIONSORT_THRESHOLD) {
      for (int i=low; i<high; i++)
        for (int j=i; j>low && c.compare(s.getKey(dest, j - 1), s.getKey(dest, j)) > 0; j--)
          s.swap(dest, j, j - 1);
      return;
    }

    // Recursively sort halves of dest into src
    int destLow  = low;
    int destHigh = high;
    low  += off;
    high += off;
    int mid = (low + high) >>> 1;
    mergeSort(dest, src, low, mid, -off, c);
    mergeSort(dest, src, mid, high, -off, c);

    // If list is already sorted, just copy from src to dest.  This is an
    // optimization that results in faster sorts for nearly ordered lists.
    if (c.compare(s.getKey(src, mid - 1), s.getKey(src, mid)) <= 0) {
      s.copyRange(src, low, dest, destLow, length);
      return;
    }

    // Merge sorted halves (now in src) into dest
    for(int i = destLow, p = low, q = mid; i < destHigh; i++) {
      if (q >= high || p < mid && c.compare(s.getKey(src, p), s.getKey(src, q)) <= 0)
        s.copyElement(src, p++, dest, i);
      else
        s.copyElement(src, q++, dest, i);
    }
  }
}
