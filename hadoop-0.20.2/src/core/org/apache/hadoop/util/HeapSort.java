/*
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
package org.apache.hadoop.util;

/**
 * An implementation of the core algorithm of HeapSort.
 */
public final class HeapSort implements IndexedSorter {

  public HeapSort() { }

  private static void downHeap(final IndexedSortable s, final int b,
      int i, final int N) {
    for (int idx = i << 1; idx < N; idx = i << 1) {
      if (idx + 1 < N && s.compare(b + idx, b + idx + 1) < 0) {
        if (s.compare(b + i, b + idx + 1) < 0) {
          s.swap(b + i, b + idx + 1);
        } else return;
        i = idx + 1;
      } else if (s.compare(b + i, b + idx) < 0) {
        s.swap(b + i, b + idx);
        i = idx;
      } else return;
    }
  }

  /**
   * Sort the given range of items using heap sort.
   * {@inheritDoc}
   */
  public void sort(IndexedSortable s, int p, int r) {
    sort(s, p, r, null);
  }

  /**
   * {@inheritDoc}
   */
  public void sort(final IndexedSortable s, final int p, final int r,
      final Progressable rep) {
    final int N = r - p;
    // build heap w/ reverse comparator, then write in-place from end
    final int t = Integer.highestOneBit(N);
    for (int i = t; i > 1; i >>>= 1) {
      for (int j = i >>> 1; j < i; ++j) {
        downHeap(s, p-1, j, N + 1);
      }
      if (null != rep) {
        rep.progress();
      }
    }
    for (int i = r - 1; i > p; --i) {
      s.swap(p, i);
      downHeap(s, p - 1, 1, i - p + 1);
    }
  }
}
