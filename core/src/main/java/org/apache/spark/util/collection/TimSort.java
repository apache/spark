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

/*
 * Based on TimSort.java from the Android Open Source Project
 *
 *  Copyright (C) 2008 The Android Open Source Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.util.collection;

import java.util.Comparator;

/**
 * A port of the Android TimSort class, which utilizes a "stable, adaptive, iterative mergesort."
 * See the method comment on sort() for more details.
 *
 * This has been kept in Java with the original style in order to match very closely with the
 * Android source code, and thus be easy to verify correctness. The class is package private. We put
 * a simple Scala wrapper {@link org.apache.spark.util.collection.Sorter}, which is available to
 * package org.apache.spark.
 *
 * The purpose of the port is to generalize the interface to the sort to accept input data formats
 * besides simple arrays where every element is sorted individually. For instance, the AppendOnlyMap
 * uses this to sort an Array with alternating elements of the form [key, value, key, value].
 * This generalization comes with minimal overhead -- see SortDataFormat for more information.
 *
 * We allow key reuse to prevent creating many key objects -- see SortDataFormat.
 *
 * @see org.apache.spark.util.collection.SortDataFormat
 * @see org.apache.spark.util.collection.Sorter
 */
class TimSort<K, Buffer> {

  /**
   * This is the minimum sized sequence that will be merged.  Shorter
   * sequences will be lengthened by calling binarySort.  If the entire
   * array is less than this length, no merges will be performed.
   *
   * This constant should be a power of two.  It was 64 in Tim Peter's C
   * implementation, but 32 was empirically determined to work better in
   * this implementation.  In the unlikely event that you set this constant
   * to be a number that's not a power of two, you'll need to change the
   * minRunLength computation.
   *
   * If you decrease this constant, you must change the stackLen
   * computation in the TimSort constructor, or you risk an
   * ArrayOutOfBounds exception.  See listsort.txt for a discussion
   * of the minimum stack length required as a function of the length
   * of the array being sorted and the minimum merge sequence length.
   */
  private static final int MIN_MERGE = 32;

  private final SortDataFormat<K, Buffer> s;

  public TimSort(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
  }

  /**
   * A stable, adaptive, iterative mergesort that requires far fewer than
   * n lg(n) comparisons when running on partially sorted arrays, while
   * offering performance comparable to a traditional mergesort when run
   * on random arrays.  Like all proper mergesorts, this sort is stable and
   * runs O(n log n) time (worst case).  In the worst case, this sort requires
   * temporary storage space for n/2 object references; in the best case,
   * it requires only a small constant amount of space.
   *
   * This implementation was adapted from Tim Peters's list sort for
   * Python, which is described in detail here:
   *
   *   http://svn.python.org/projects/python/trunk/Objects/listsort.txt
   *
   * Tim's C code may be found here:
   *
   *   http://svn.python.org/projects/python/trunk/Objects/listobject.c
   *
   * The underlying techniques are described in this paper (and may have
   * even earlier origins):
   *
   *  "Optimistic Sorting and Information Theoretic Complexity"
   *  Peter McIlroy
   *  SODA (Fourth Annual ACM-SIAM Symposium on Discrete Algorithms),
   *  pp 467-474, Austin, Texas, 25-27 January 1993.
   *
   * While the API to this class consists solely of static methods, it is
   * (privately) instantiable; a TimSort instance holds the state of an ongoing
   * sort, assuming the input array is large enough to warrant the full-blown
   * TimSort. Small arrays are sorted in place, using a binary insertion sort.
   *
   * @author Josh Bloch
   */
  public void sort(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert c != null;

    int nRemaining  = hi - lo;
    if (nRemaining < 2)
      return;  // Arrays of size 0 and 1 are always sorted

    // If array is small, do a "mini-TimSort" with no merges
    if (nRemaining < MIN_MERGE) {
      int initRunLen = countRunAndMakeAscending(a, lo, hi, c);
      binarySort(a, lo, hi, lo + initRunLen, c);
      return;
    }

    /**
     * March over the array once, left to right, finding natural runs,
     * extending short natural runs to minRun elements, and merging runs
     * to maintain stack invariant.
     */
    SortState sortState = new SortState(a, c, hi - lo);
    int minRun = minRunLength(nRemaining);
    do {
      // Identify next run
      int runLen = countRunAndMakeAscending(a, lo, hi, c);

      // If run is short, extend to min(minRun, nRemaining)
      if (runLen < minRun) {
        int force = nRemaining <= minRun ? nRemaining : minRun;
        binarySort(a, lo, lo + force, lo + runLen, c);
        runLen = force;
      }

      // Push run onto pending-run stack, and maybe merge
      sortState.pushRun(lo, runLen);
      sortState.mergeCollapse();

      // Advance to find next run
      lo += runLen;
      nRemaining -= runLen;
    } while (nRemaining != 0);

    // Merge all remaining runs to complete sort
    assert lo == hi;
    sortState.mergeForceCollapse();
    assert sortState.stackSize == 1;
  }

  /**
   * Sorts the specified portion of the specified array using a binary
   * insertion sort.  This is the best method for sorting small numbers
   * of elements.  It requires O(n log n) compares, but O(n^2) data
   * movement (worst case).
   *
   * If the initial part of the specified range is already sorted,
   * this method can take advantage of it: the method assumes that the
   * elements from index {@code lo}, inclusive, to {@code start},
   * exclusive are already sorted.
   *
   * @param a the array in which a range is to be sorted
   * @param lo the index of the first element in the range to be sorted
   * @param hi the index after the last element in the range to be sorted
   * @param start the index of the first element in the range that is
   *        not already known to be sorted ({@code lo <= start <= hi})
   * @param c comparator to used for the sort
   */
  @SuppressWarnings("fallthrough")
  private void binarySort(Buffer a, int lo, int hi, int start, Comparator<? super K> c) {
    assert lo <= start && start <= hi;
    if (start == lo)
      start++;

    K key0 = s.newKey();
    K key1 = s.newKey();

    Buffer pivotStore = s.allocate(1);
    for ( ; start < hi; start++) {
      s.copyElement(a, start, pivotStore, 0);
      K pivot = s.getKey(pivotStore, 0, key0);

      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (c.compare(pivot, s.getKey(a, mid, key1)) < 0)
          right = mid;
        else
          left = mid + 1;
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left;  // The number of elements to move
      // Switch is just an optimization for arraycopy in default case
      switch (n) {
        case 2:  s.copyElement(a, left + 1, a, left + 2);
        case 1:  s.copyElement(a, left, a, left + 1);
          break;
        default: s.copyRange(a, left, a, left + 1, n);
      }
      s.copyElement(pivotStore, 0, a, left);
    }
  }

  /**
   * Returns the length of the run beginning at the specified position in
   * the specified array and reverses the run if it is descending (ensuring
   * that the run will always be ascending when the method returns).
   *
   * A run is the longest ascending sequence with:
   *
   *    a[lo] <= a[lo + 1] <= a[lo + 2] <= ...
   *
   * or the longest descending sequence with:
   *
   *    a[lo] >  a[lo + 1] >  a[lo + 2] >  ...
   *
   * For its intended use in a stable mergesort, the strictness of the
   * definition of "descending" is needed so that the call can safely
   * reverse a descending sequence without violating stability.
   *
   * @param a the array in which a run is to be counted and possibly reversed
   * @param lo index of the first element in the run
   * @param hi index after the last element that may be contained in the run.
  It is required that {@code lo < hi}.
   * @param c the comparator to used for the sort
   * @return  the length of the run beginning at the specified position in
   *          the specified array
   */
  private int countRunAndMakeAscending(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi)
      return 1;

    K key0 = s.newKey();
    K key1 = s.newKey();

    // Find end of run, and reverse range if descending
    if (c.compare(s.getKey(a, runHi++, key0), s.getKey(a, lo, key1)) < 0) { // Descending
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) < 0)
        runHi++;
      reverseRange(a, lo, runHi);
    } else {                              // Ascending
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) >= 0)
        runHi++;
    }

    return runHi - lo;
  }

  /**
   * Reverse the specified range of the specified array.
   *
   * @param a the array in which a range is to be reversed
   * @param lo the index of the first element in the range to be reversed
   * @param hi the index after the last element in the range to be reversed
   */
  private void reverseRange(Buffer a, int lo, int hi) {
    hi--;
    while (lo < hi) {
      s.swap(a, lo, hi);
      lo++;
      hi--;
    }
  }

  /**
   * Returns the minimum acceptable run length for an array of the specified
   * length. Natural runs shorter than this will be extended with
   * {@link #binarySort}.
   *
   * Roughly speaking, the computation is:
   *
   *  If n < MIN_MERGE, return n (it's too small to bother with fancy stuff).
   *  Else if n is an exact power of 2, return MIN_MERGE/2.
   *  Else return an int k, MIN_MERGE/2 <= k <= MIN_MERGE, such that n/k
   *   is close to, but strictly less than, an exact power of 2.
   *
   * For the rationale, see listsort.txt.
   *
   * @param n the length of the array to be sorted
   * @return the length of the minimum run to be merged
   */
  private int minRunLength(int n) {
    assert n >= 0;
    int r = 0;      // Becomes 1 if any 1 bits are shifted off
    while (n >= MIN_MERGE) {
      r |= (n & 1);
      n >>= 1;
    }
    return n + r;
  }

  private class SortState {

    /**
     * The Buffer being sorted.
     */
    private final Buffer a;

    /**
     * Length of the sort Buffer.
     */
    private final int aLength;

    /**
     * The comparator for this sort.
     */
    private final Comparator<? super K> c;

    /**
     * When we get into galloping mode, we stay there until both runs win less
     * often than MIN_GALLOP consecutive times.
     */
    private static final int  MIN_GALLOP = 7;

    /**
     * This controls when we get *into* galloping mode.  It is initialized
     * to MIN_GALLOP.  The mergeLo and mergeHi methods nudge it higher for
     * random data, and lower for highly structured data.
     */
    private int minGallop = MIN_GALLOP;

    /**
     * Maximum initial size of tmp array, which is used for merging.  The array
     * can grow to accommodate demand.
     *
     * Unlike Tim's original C version, we do not allocate this much storage
     * when sorting smaller arrays.  This change was required for performance.
     */
    private static final int INITIAL_TMP_STORAGE_LENGTH = 256;

    /**
     * Temp storage for merges.
     */
    private Buffer tmp; // Actual runtime type will be Object[], regardless of T

    /**
     * Length of the temp storage.
     */
    private int tmpLength = 0;

    /**
     * A stack of pending runs yet to be merged.  Run i starts at
     * address base[i] and extends for len[i] elements.  It's always
     * true (so long as the indices are in bounds) that:
     *
     *     runBase[i] + runLen[i] == runBase[i + 1]
     *
     * so we could cut the storage for this, but it's a minor amount,
     * and keeping all the info explicit simplifies the code.
     */
    private int stackSize = 0;  // Number of pending runs on stack
    private final int[] runBase;
    private final int[] runLen;

    /**
     * Creates a TimSort instance to maintain the state of an ongoing sort.
     *
     * @param a the array to be sorted
     * @param c the comparator to determine the order of the sort
     */
    private SortState(Buffer a, Comparator<? super K> c, int len) {
      this.aLength = len;
      this.a = a;
      this.c = c;

      // Allocate temp storage (which may be increased later if necessary)
      tmpLength = len < 2 * INITIAL_TMP_STORAGE_LENGTH ? len >>> 1 : INITIAL_TMP_STORAGE_LENGTH;
      tmp = s.allocate(tmpLength);

      /*
       * Allocate runs-to-be-merged stack (which cannot be expanded).  The
       * stack length requirements are described in listsort.txt.  The C
       * version always uses the same stack length (85), but this was
       * measured to be too expensive when sorting "mid-sized" arrays (e.g.,
       * 100 elements) in Java.  Therefore, we use smaller (but sufficiently
       * large) stack lengths for smaller arrays.  The "magic numbers" in the
       * computation below must be changed if MIN_MERGE is decreased.  See
       * the MIN_MERGE declaration above for more information.
       */
      int stackLen = (len <    120  ?  5 :
                      len <   1542  ? 10 :
                      len < 119151  ? 19 : 40);
      runBase = new int[stackLen];
      runLen = new int[stackLen];
    }

    /**
     * Pushes the specified run onto the pending-run stack.
     *
     * @param runBase index of the first element in the run
     * @param runLen  the number of elements in the run
     */
    private void pushRun(int runBase, int runLen) {
      this.runBase[stackSize] = runBase;
      this.runLen[stackSize] = runLen;
      stackSize++;
    }

    /**
     * Examines the stack of runs waiting to be merged and merges adjacent runs
     * until the stack invariants are reestablished:
     *
     *     1. runLen[i - 3] > runLen[i - 2] + runLen[i - 1]
     *     2. runLen[i - 2] > runLen[i - 1]
     *
     * This method is called each time a new run is pushed onto the stack,
     * so the invariants are guaranteed to hold for i < stackSize upon
     * entry to the method.
     */
    private void mergeCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if ( (n >= 1 && runLen[n-1] <= runLen[n] + runLen[n+1])
          || (n >= 2 && runLen[n-2] <= runLen[n] + runLen[n-1])) {
          if (runLen[n - 1] < runLen[n + 1])
            n--;
        } else if (runLen[n] > runLen[n + 1]) {
          break; // Invariant is established
        }
        mergeAt(n);
      }
    }

    /**
     * Merges all runs on the stack until only one remains.  This method is
     * called once, to complete the sort.
     */
    private void mergeForceCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if (n > 0 && runLen[n - 1] < runLen[n + 1])
          n--;
        mergeAt(n);
      }
    }

    /**
     * Merges the two runs at stack indices i and i+1.  Run i must be
     * the penultimate or antepenultimate run on the stack.  In other words,
     * i must be equal to stackSize-2 or stackSize-3.
     *
     * @param i stack index of the first of the two runs to merge
     */
    private void mergeAt(int i) {
      assert stackSize >= 2;
      assert i >= 0;
      assert i == stackSize - 2 || i == stackSize - 3;

      int base1 = runBase[i];
      int len1 = runLen[i];
      int base2 = runBase[i + 1];
      int len2 = runLen[i + 1];
      assert len1 > 0 && len2 > 0;
      assert base1 + len1 == base2;

      /*
       * Record the length of the combined runs; if i is the 3rd-last
       * run now, also slide over the last run (which isn't involved
       * in this merge).  The current run (i+1) goes away in any case.
       */
      runLen[i] = len1 + len2;
      if (i == stackSize - 3) {
        runBase[i + 1] = runBase[i + 2];
        runLen[i + 1] = runLen[i + 2];
      }
      stackSize--;

      K key0 = s.newKey();

      /*
       * Find where the first element of run2 goes in run1. Prior elements
       * in run1 can be ignored (because they're already in place).
       */
      int k = gallopRight(s.getKey(a, base2, key0), a, base1, len1, 0, c);
      assert k >= 0;
      base1 += k;
      len1 -= k;
      if (len1 == 0)
        return;

      /*
       * Find where the last element of run1 goes in run2. Subsequent elements
       * in run2 can be ignored (because they're already in place).
       */
      len2 = gallopLeft(s.getKey(a, base1 + len1 - 1, key0), a, base2, len2, len2 - 1, c);
      assert len2 >= 0;
      if (len2 == 0)
        return;

      // Merge remaining runs, using tmp array with min(len1, len2) elements
      if (len1 <= len2)
        mergeLo(base1, len1, base2, len2);
      else
        mergeHi(base1, len1, base2, len2);
    }

    /**
     * Locates the position at which to insert the specified key into the
     * specified sorted range; if the range contains an element equal to key,
     * returns the index of the leftmost equal element.
     *
     * @param key the key whose insertion point to search for
     * @param a the array in which to search
     * @param base the index of the first element in the range
     * @param len the length of the range; must be > 0
     * @param hint the index at which to begin the search, 0 <= hint < n.
     *     The closer hint is to the result, the faster this method will run.
     * @param c the comparator used to order the range, and to search
     * @return the int k,  0 <= k <= n such that a[b + k - 1] < key <= a[b + k],
     *    pretending that a[b - 1] is minus infinity and a[b + n] is infinity.
     *    In other words, key belongs at index b + k; or in other words,
     *    the first k elements of a should precede key, and the last n - k
     *    should follow it.
     */
    private int gallopLeft(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
      assert len > 0 && hint >= 0 && hint < len;
      int lastOfs = 0;
      int ofs = 1;
      K key0 = s.newKey();

      if (c.compare(key, s.getKey(a, base + hint, key0)) > 0) {
        // Gallop right until a[base+hint+lastOfs] < key <= a[base+hint+ofs]
        int maxOfs = len - hint;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs, key0)) > 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to base
        lastOfs += hint;
        ofs += hint;
      } else { // key <= a[base + hint]
        // Gallop left until a[base+hint-ofs] < key <= a[base+hint-lastOfs]
        final int maxOfs = hint + 1;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key0)) <= 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to base
        int tmp = lastOfs;
        lastOfs = hint - ofs;
        ofs = hint - tmp;
      }
      assert -1 <= lastOfs && lastOfs < ofs && ofs <= len;

      /*
       * Now a[base+lastOfs] < key <= a[base+ofs], so key belongs somewhere
       * to the right of lastOfs but no farther right than ofs.  Do a binary
       * search, with invariant a[base + lastOfs - 1] < key <= a[base + ofs].
       */
      lastOfs++;
      while (lastOfs < ofs) {
        int m = lastOfs + ((ofs - lastOfs) >>> 1);

        if (c.compare(key, s.getKey(a, base + m, key0)) > 0)
          lastOfs = m + 1;  // a[base + m] < key
        else
          ofs = m;          // key <= a[base + m]
      }
      assert lastOfs == ofs;    // so a[base + ofs - 1] < key <= a[base + ofs]
      return ofs;
    }

    /**
     * Like gallopLeft, except that if the range contains an element equal to
     * key, gallopRight returns the index after the rightmost equal element.
     *
     * @param key the key whose insertion point to search for
     * @param a the array in which to search
     * @param base the index of the first element in the range
     * @param len the length of the range; must be > 0
     * @param hint the index at which to begin the search, 0 <= hint < n.
     *     The closer hint is to the result, the faster this method will run.
     * @param c the comparator used to order the range, and to search
     * @return the int k,  0 <= k <= n such that a[b + k - 1] <= key < a[b + k]
     */
    private int gallopRight(K key, Buffer a, int base, int len, int hint, Comparator<? super K> c) {
      assert len > 0 && hint >= 0 && hint < len;

      int ofs = 1;
      int lastOfs = 0;
      K key1 = s.newKey();

      if (c.compare(key, s.getKey(a, base + hint, key1)) < 0) {
        // Gallop left until a[b+hint - ofs] <= key < a[b+hint - lastOfs]
        int maxOfs = hint + 1;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint - ofs, key1)) < 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to b
        int tmp = lastOfs;
        lastOfs = hint - ofs;
        ofs = hint - tmp;
      } else { // a[b + hint] <= key
        // Gallop right until a[b+hint + lastOfs] <= key < a[b+hint + ofs]
        int maxOfs = len - hint;
        while (ofs < maxOfs && c.compare(key, s.getKey(a, base + hint + ofs, key1)) >= 0) {
          lastOfs = ofs;
          ofs = (ofs << 1) + 1;
          if (ofs <= 0)   // int overflow
            ofs = maxOfs;
        }
        if (ofs > maxOfs)
          ofs = maxOfs;

        // Make offsets relative to b
        lastOfs += hint;
        ofs += hint;
      }
      assert -1 <= lastOfs && lastOfs < ofs && ofs <= len;

      /*
       * Now a[b + lastOfs] <= key < a[b + ofs], so key belongs somewhere to
       * the right of lastOfs but no farther right than ofs.  Do a binary
       * search, with invariant a[b + lastOfs - 1] <= key < a[b + ofs].
       */
      lastOfs++;
      while (lastOfs < ofs) {
        int m = lastOfs + ((ofs - lastOfs) >>> 1);

        if (c.compare(key, s.getKey(a, base + m, key1)) < 0)
          ofs = m;          // key < a[b + m]
        else
          lastOfs = m + 1;  // a[b + m] <= key
      }
      assert lastOfs == ofs;    // so a[b + ofs - 1] <= key < a[b + ofs]
      return ofs;
    }

    /**
     * Merges two adjacent runs in place, in a stable fashion.  The first
     * element of the first run must be greater than the first element of the
     * second run (a[base1] > a[base2]), and the last element of the first run
     * (a[base1 + len1-1]) must be greater than all elements of the second run.
     *
     * For performance, this method should be called only when len1 <= len2;
     * its twin, mergeHi should be called if len1 >= len2.  (Either method
     * may be called if len1 == len2.)
     *
     * @param base1 index of first element in first run to be merged
     * @param len1  length of first run to be merged (must be > 0)
     * @param base2 index of first element in second run to be merged
     *        (must be aBase + aLen)
     * @param len2  length of second run to be merged (must be > 0)
     */
    private void mergeLo(int base1, int len1, int base2, int len2) {
      assert len1 > 0 && len2 > 0 && base1 + len1 == base2;

      // Copy first run into temp array
      Buffer a = this.a; // For performance
      Buffer tmp = ensureCapacity(len1);
      s.copyRange(a, base1, tmp, 0, len1);

      int cursor1 = 0;       // Indexes into tmp array
      int cursor2 = base2;   // Indexes int a
      int dest = base1;      // Indexes int a

      // Move first element of second run and deal with degenerate cases
      s.copyElement(a, cursor2++, a, dest++);
      if (--len2 == 0) {
        s.copyRange(tmp, cursor1, a, dest, len1);
        return;
      }
      if (len1 == 1) {
        s.copyRange(a, cursor2, a, dest, len2);
        s.copyElement(tmp, cursor1, a, dest + len2); // Last elt of run 1 to end of merge
        return;
      }

      K key0 = s.newKey();
      K key1 = s.newKey();

      Comparator<? super K> c = this.c;  // Use local variable for performance
      int minGallop = this.minGallop;    //  "    "       "     "      "
      outer:
      while (true) {
        int count1 = 0; // Number of times in a row that first run won
        int count2 = 0; // Number of times in a row that second run won

        /*
         * Do the straightforward thing until (if ever) one run starts
         * winning consistently.
         */
        do {
          assert len1 > 1 && len2 > 0;
          if (c.compare(s.getKey(a, cursor2, key0), s.getKey(tmp, cursor1, key1)) < 0) {
            s.copyElement(a, cursor2++, a, dest++);
            count2++;
            count1 = 0;
            if (--len2 == 0)
              break outer;
          } else {
            s.copyElement(tmp, cursor1++, a, dest++);
            count1++;
            count2 = 0;
            if (--len1 == 1)
              break outer;
          }
        } while ((count1 | count2) < minGallop);

        /*
         * One run is winning so consistently that galloping may be a
         * huge win. So try that, and continue galloping until (if ever)
         * neither run appears to be winning consistently anymore.
         */
        do {
          assert len1 > 1 && len2 > 0;
          count1 = gallopRight(s.getKey(a, cursor2, key0), tmp, cursor1, len1, 0, c);
          if (count1 != 0) {
            s.copyRange(tmp, cursor1, a, dest, count1);
            dest += count1;
            cursor1 += count1;
            len1 -= count1;
            if (len1 <= 1) // len1 == 1 || len1 == 0
              break outer;
          }
          s.copyElement(a, cursor2++, a, dest++);
          if (--len2 == 0)
            break outer;

          count2 = gallopLeft(s.getKey(tmp, cursor1, key0), a, cursor2, len2, 0, c);
          if (count2 != 0) {
            s.copyRange(a, cursor2, a, dest, count2);
            dest += count2;
            cursor2 += count2;
            len2 -= count2;
            if (len2 == 0)
              break outer;
          }
          s.copyElement(tmp, cursor1++, a, dest++);
          if (--len1 == 1)
            break outer;
          minGallop--;
        } while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);
        if (minGallop < 0)
          minGallop = 0;
        minGallop += 2;  // Penalize for leaving gallop mode
      }  // End of "outer" loop
      this.minGallop = minGallop < 1 ? 1 : minGallop;  // Write back to field

      if (len1 == 1) {
        assert len2 > 0;
        s.copyRange(a, cursor2, a, dest, len2);
        s.copyElement(tmp, cursor1, a, dest + len2); //  Last elt of run 1 to end of merge
      } else if (len1 == 0) {
        throw new IllegalArgumentException(
            "Comparison method violates its general contract!");
      } else {
        assert len2 == 0;
        assert len1 > 1;
        s.copyRange(tmp, cursor1, a, dest, len1);
      }
    }

    /**
     * Like mergeLo, except that this method should be called only if
     * len1 >= len2; mergeLo should be called if len1 <= len2.  (Either method
     * may be called if len1 == len2.)
     *
     * @param base1 index of first element in first run to be merged
     * @param len1  length of first run to be merged (must be > 0)
     * @param base2 index of first element in second run to be merged
     *        (must be aBase + aLen)
     * @param len2  length of second run to be merged (must be > 0)
     */
    private void mergeHi(int base1, int len1, int base2, int len2) {
      assert len1 > 0 && len2 > 0 && base1 + len1 == base2;

      // Copy second run into temp array
      Buffer a = this.a; // For performance
      Buffer tmp = ensureCapacity(len2);
      s.copyRange(a, base2, tmp, 0, len2);

      int cursor1 = base1 + len1 - 1;  // Indexes into a
      int cursor2 = len2 - 1;          // Indexes into tmp array
      int dest = base2 + len2 - 1;     // Indexes into a

      K key0 = s.newKey();
      K key1 = s.newKey();

      // Move last element of first run and deal with degenerate cases
      s.copyElement(a, cursor1--, a, dest--);
      if (--len1 == 0) {
        s.copyRange(tmp, 0, a, dest - (len2 - 1), len2);
        return;
      }
      if (len2 == 1) {
        dest -= len1;
        cursor1 -= len1;
        s.copyRange(a, cursor1 + 1, a, dest + 1, len1);
        s.copyElement(tmp, cursor2, a, dest);
        return;
      }

      Comparator<? super K> c = this.c;  // Use local variable for performance
      int minGallop = this.minGallop;    //  "    "       "     "      "
      outer:
      while (true) {
        int count1 = 0; // Number of times in a row that first run won
        int count2 = 0; // Number of times in a row that second run won

        /*
         * Do the straightforward thing until (if ever) one run
         * appears to win consistently.
         */
        do {
          assert len1 > 0 && len2 > 1;
          if (c.compare(s.getKey(tmp, cursor2, key0), s.getKey(a, cursor1, key1)) < 0) {
            s.copyElement(a, cursor1--, a, dest--);
            count1++;
            count2 = 0;
            if (--len1 == 0)
              break outer;
          } else {
            s.copyElement(tmp, cursor2--, a, dest--);
            count2++;
            count1 = 0;
            if (--len2 == 1)
              break outer;
          }
        } while ((count1 | count2) < minGallop);

        /*
         * One run is winning so consistently that galloping may be a
         * huge win. So try that, and continue galloping until (if ever)
         * neither run appears to be winning consistently anymore.
         */
        do {
          assert len1 > 0 && len2 > 1;
          count1 = len1 - gallopRight(s.getKey(tmp, cursor2, key0), a, base1, len1, len1 - 1, c);
          if (count1 != 0) {
            dest -= count1;
            cursor1 -= count1;
            len1 -= count1;
            s.copyRange(a, cursor1 + 1, a, dest + 1, count1);
            if (len1 == 0)
              break outer;
          }
          s.copyElement(tmp, cursor2--, a, dest--);
          if (--len2 == 1)
            break outer;

          count2 = len2 - gallopLeft(s.getKey(a, cursor1, key0), tmp, 0, len2, len2 - 1, c);
          if (count2 != 0) {
            dest -= count2;
            cursor2 -= count2;
            len2 -= count2;
            s.copyRange(tmp, cursor2 + 1, a, dest + 1, count2);
            if (len2 <= 1)  // len2 == 1 || len2 == 0
              break outer;
          }
          s.copyElement(a, cursor1--, a, dest--);
          if (--len1 == 0)
            break outer;
          minGallop--;
        } while (count1 >= MIN_GALLOP | count2 >= MIN_GALLOP);
        if (minGallop < 0)
          minGallop = 0;
        minGallop += 2;  // Penalize for leaving gallop mode
      }  // End of "outer" loop
      this.minGallop = minGallop < 1 ? 1 : minGallop;  // Write back to field

      if (len2 == 1) {
        assert len1 > 0;
        dest -= len1;
        cursor1 -= len1;
        s.copyRange(a, cursor1 + 1, a, dest + 1, len1);
        s.copyElement(tmp, cursor2, a, dest); // Move first elt of run2 to front of merge
      } else if (len2 == 0) {
        throw new IllegalArgumentException(
            "Comparison method violates its general contract!");
      } else {
        assert len1 == 0;
        assert len2 > 0;
        s.copyRange(tmp, 0, a, dest - (len2 - 1), len2);
      }
    }

    /**
     * Ensures that the external array tmp has at least the specified
     * number of elements, increasing its size if necessary.  The size
     * increases exponentially to ensure amortized linear time complexity.
     *
     * @param minCapacity the minimum required capacity of the tmp array
     * @return tmp, whether or not it grew
     */
    private Buffer ensureCapacity(int minCapacity) {
      if (tmpLength < minCapacity) {
        // Compute smallest power of 2 > minCapacity
        int newSize = minCapacity;
        newSize |= newSize >> 1;
        newSize |= newSize >> 2;
        newSize |= newSize >> 4;
        newSize |= newSize >> 8;
        newSize |= newSize >> 16;
        newSize++;

        if (newSize < 0) // Not bloody likely!
          newSize = minCapacity;
        else
          newSize = Math.min(newSize, aLength >>> 1);

        tmp = s.allocate(newSize);
        tmpLength = newSize;
      }
      return tmp;
    }
  }
}
