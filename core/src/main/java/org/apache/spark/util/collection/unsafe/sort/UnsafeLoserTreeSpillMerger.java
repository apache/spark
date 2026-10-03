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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.IOException;
import java.util.Arrays;

/**
 * A k-way merge implementation backed by a classic loser tree.
 *
 * Compared to a binary heap (sift-down), each replay along the path from a
 * leaf to the root performs only a single comparison and a single loser read
 * per level -- heap sift-down does two comparisons plus two child reads per
 * level. For k-way merges, this approximately halves both the comparison
 * count and the array touches per popped record.
 *
 * Layout:
 *   loser[0]                        -- the overall winner (run id)
 *   loser[1 .. treeSize-1]          -- the loser at each internal node
 *   leaves are virtual; leaf i (for i in [0, capacity)) maps to run id i
 */
final class UnsafeLoserTreeSpillMerger {

  private final RecordComparator recordComparator;
  private final PrefixComparator prefixComparator;
  private final UnsafeSorterIterator[] iterators;
  // exhausted[capacity] is the always-empty sentinel slot.
  private final boolean[] exhausted;
  private final int[] loser;
  private final int treeSize;
  private final int sentinel;

  private int numIterators = 0;
  private int numRecords = 0;
  private boolean treeBuilt = false;

  UnsafeLoserTreeSpillMerger(
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int numSpills) {
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    int capacity = Math.max(1, numSpills);
    this.iterators = new UnsafeSorterIterator[capacity];
    this.exhausted = new boolean[capacity + 1];
    Arrays.fill(exhausted, true);
    this.sentinel = capacity;
    this.treeSize = nextPowerOfTwo(capacity);
    this.loser = new int[treeSize];
    Arrays.fill(loser, sentinel);
  }

  /**
   * Add an UnsafeSorterIterator to this merger.
   */
  public void addSpillIfNotEmpty(UnsafeSorterIterator spillReader) throws IOException {
    if (spillReader.hasNext()) {
      spillReader.loadNext();
      iterators[numIterators] = spillReader;
      exhausted[numIterators] = false;
      numIterators++;
      numRecords += spillReader.getNumRecords();
    }
  }

  public UnsafeSorterIterator getSortedIterator() {
    buildTree();
    return new MergedIterator();
  }

  private void buildTree() {
    if (treeBuilt) {
      return;
    }
    if (treeSize == 1) {
      loser[0] = exhausted[0] ? sentinel : 0;
    } else {
      loser[0] = buildSubtree(1);
    }
    treeBuilt = true;
  }

  // Bottom-up build: returns the winner of the subtree rooted at `node` and
  // stores the loser of that subtree at loser[node]. Virtual leaves beyond
  // capacity collapse to the sentinel run id, which loses every comparison.
  private int buildSubtree(int node) {
    if (node >= treeSize) {
      int runId = node - treeSize;
      return Math.min(runId, sentinel);
    }
    int leftWinner = buildSubtree(node << 1);
    int rightWinner = buildSubtree((node << 1) | 1);
    if (isLess(leftWinner, rightWinner)) {
      loser[node] = rightWinner;
      return leftWinner;
    } else {
      loser[node] = leftWinner;
      return rightWinner;
    }
  }

  // Replay the path from `runId`'s leaf up to the root after that leaf's key
  // changed. One comparison + one loser-slot read per level.
  private void replay(int runId) {
    int winner = runId;
    int node = (treeSize + runId) >> 1;
    while (node > 0) {
      int other = loser[node];
      if (isLess(other, winner)) {
        loser[node] = winner;
        winner = other;
      }
      node >>= 1;
    }
    loser[0] = winner;
  }

  private boolean isLess(int left, int right) {
    boolean leftEmpty = exhausted[left];
    boolean rightEmpty = exhausted[right];
    if (leftEmpty) {
      return false;
    }
    if (rightEmpty) {
      return true;
    }
    UnsafeSorterIterator l = iterators[left];
    UnsafeSorterIterator r = iterators[right];
    int prefixCmp = prefixComparator.compare(l.getKeyPrefix(), r.getKeyPrefix());
    if (prefixCmp != 0) {
      return prefixCmp < 0;
    }
    int recCmp = recordComparator.compare(
        l.getBaseObject(), l.getBaseOffset(), l.getRecordLength(),
        r.getBaseObject(), r.getBaseOffset(), r.getRecordLength());
    if (recCmp != 0) {
      return recCmp < 0;
    }
    // Stable tie-break by run id (lower id wins). This differs from the
    // original PriorityQueue which has no stable tie-break on equal keys.
    return left < right;
  }

  private static int nextPowerOfTwo(int v) {
    int s = 1;
    while (s < v) {
      s <<= 1;
    }
    return s;
  }

  private final class MergedIterator extends UnsafeSorterIterator {
    private UnsafeSorterIterator currentReader;
    private int currentWinner = -1;
    private boolean hasNext;
    private int exhaustedCount = 0;

    MergedIterator() {
      hasNext = (numIterators > 0);
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public long getCurrentPageNumber() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public void loadNext() throws IOException {
      if (currentWinner != -1) {
        UnsafeSorterIterator prev = iterators[currentWinner];
        if (prev.hasNext()) {
          prev.loadNext();
        } else {
          exhausted[currentWinner] = true;
          exhaustedCount++;
        }
        replay(currentWinner);
      }
      currentWinner = loser[0];
      currentReader = iterators[currentWinner];
      // After this record, is there more?
      // Yes if: current iterator has more records, or other iterators exist.
      hasNext = iterators[currentWinner].hasNext()
          || exhaustedCount < numIterators - 1;
    }

    @Override
    public Object getBaseObject() { return currentReader.getBaseObject(); }

    @Override
    public long getBaseOffset() { return currentReader.getBaseOffset(); }

    @Override
    public int getRecordLength() { return currentReader.getRecordLength(); }

    @Override
    public long getKeyPrefix() { return currentReader.getKeyPrefix(); }
  }
}
