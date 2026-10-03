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
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Merges the sorted runs of an external sort (the spill files plus any in-memory run) into one
 * sorted stream. Runs are ordered by the 8-byte key prefix, falling back to the
 * {@link RecordComparator} on a prefix tie.
 *
 * <p>The merge structure is chosen by the number of runs {@code k}. For a small {@code k} a linear
 * min-scan (O(k) per record) is used; above {@link #LINEAR_MERGE_MAX_RUNS} a loser (tournament)
 * tree is used, which does about log2(k) comparisons and a single replace-top adjustment per
 * record. Both replace the previous {@code PriorityQueue} heap, whose poll+add pattern does two
 * sift passes per record; a microbenchmark of the prefix-only merge showed the loser tree about
 * 1.9x faster than the heap across k, and the linear scan faster still for small k (crossover
 * around a dozen runs). That is merge CPU only -- on a real forced-spill merge, per-record I/O and
 * record decoding dominate, so the end-to-end effect is smaller and should be measured directly.
 */
public final class UnsafeSorterSpillMerger {

  /**
   * At or below this many runs use a linear min-scan; above it, a loser tree. With cached-prefix
   * comparisons the linear scan beats the tree's per-record bookkeeping up to about a dozen runs
   * (measured), so 8 stays on the faster side while covering common spill counts.
   */
  private static final int LINEAR_MERGE_MAX_RUNS = 8;

  // Total records across all runs. A long (not int) so a merge of more than Integer.MAX_VALUE
  // records terminates correctly rather than overflowing to a negative count that stops the merge
  // immediately. getNumRecords() still returns an int per the UnsafeSorterIterator contract; that
  // value is informational only (never used to size a buffer), so its truncation is harmless.
  private long numRecords = 0L;
  private final RecordComparator recordComparator;
  private final PrefixComparator prefixComparator;
  private final List<UnsafeSorterIterator> readers;
  // The readers are stateful and consumed by getSortedIterator(), so it is single-use.
  private boolean built = false;

  public UnsafeSorterSpillMerger(
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int numSpills) {
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    this.readers = new ArrayList<>(numSpills);
  }

  /**
   * Add an UnsafeSorterIterator to this merger.
   */
  public void addSpillIfNotEmpty(UnsafeSorterIterator spillReader) throws IOException {
    if (spillReader.hasNext()) {
      // We only keep non-empty readers, and each is positioned on its first record so the merge
      // can compare run heads. Keeping empty readers would make the returned iterator's hasNext()
      // over-report (it would stay true at least once per reader) and emit spurious records.
      spillReader.loadNext();
      readers.add(spillReader);
      numRecords += spillReader.getNumRecords();
    }
  }

  /** Orders two readers by their current record: key prefix first, then the record comparator. */
  private int compareCurrent(
      long leftPrefix, UnsafeSorterIterator left,
      long rightPrefix, UnsafeSorterIterator right) {
    int prefixComparisonResult = prefixComparator.compare(leftPrefix, rightPrefix);
    if (prefixComparisonResult == 0) {
      return recordComparator.compare(
        left.getBaseObject(), left.getBaseOffset(), left.getRecordLength(),
        right.getBaseObject(), right.getBaseOffset(), right.getRecordLength());
    }
    return prefixComparisonResult;
  }

  /**
   * Build the merged, sorted iterator over the added runs. Single-use: the runs are stateful, so
   * this may be called at most once.
   */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    if (built) {
      throw new IllegalStateException("getSortedIterator() may only be called once");
    }
    built = true;
    if (readers.isEmpty()) {
      return new EmptyMerger();
    } else if (readers.size() <= LINEAR_MERGE_MAX_RUNS) {
      return new LinearMerger();
    } else {
      return new LoserTreeMerger();
    }
  }

  /** No runs: an empty stream. */
  private final class EmptyMerger extends UnsafeSorterIterator {
    @Override public int getNumRecords() { return 0; }
    @Override public long getCurrentPageNumber() { throw new UnsupportedOperationException(); }
    @Override public boolean hasNext() { return false; }
    @Override public void loadNext() { throw new NoSuchElementException(); }
    @Override public Object getBaseObject() { throw new IllegalStateException(); }
    @Override public long getBaseOffset() { throw new IllegalStateException(); }
    @Override public int getRecordLength() { throw new IllegalStateException(); }
    @Override public long getKeyPrefix() { throw new IllegalStateException(); }
  }

  /**
   * Linear min-scan merge for a small number of runs. Live runs are held compacted in
   * {@code runs[0, numLive)}; the just-emitted winner is advanced (or dropped when exhausted) at
   * the start of the next {@link #loadNext()}, so the exposed record stays valid until then.
   */
  private final class LinearMerger extends UnsafeSorterIterator {
    private final UnsafeSorterIterator[] runs;
    private final long[] prefixes;
    private int numLive;
    private long emitted = 0L;
    private int winnerIdx = -1;
    private UnsafeSorterIterator current;

    LinearMerger() {
      runs = readers.toArray(new UnsafeSorterIterator[0]);
      prefixes = new long[runs.length];
      for (int i = 0; i < runs.length; i++) {
        prefixes[i] = runs[i].getKeyPrefix();
      }
      numLive = runs.length;
    }

    @Override public int getNumRecords() { return (int) numRecords; }
    @Override public long getCurrentPageNumber() { throw new UnsupportedOperationException(); }
    @Override public boolean hasNext() { return emitted < numRecords; }

    @Override
    public void loadNext() throws IOException {
      if (winnerIdx != -1) {
        UnsafeSorterIterator winner = runs[winnerIdx];
        if (winner.hasNext()) {
          winner.loadNext();
          prefixes[winnerIdx] = winner.getKeyPrefix();
        } else {
          // Drop the exhausted run by swapping in the last live run.
          runs[winnerIdx] = runs[--numLive];
          prefixes[winnerIdx] = prefixes[numLive];
          runs[numLive] = null;
        }
      }
      int minIdx = 0;
      for (int i = 1; i < numLive; i++) {
        if (compareCurrent(prefixes[i], runs[i], prefixes[minIdx], runs[minIdx]) < 0) {
          minIdx = i;
        }
      }
      winnerIdx = minIdx;
      current = runs[minIdx];
      emitted++;
    }

    @Override public Object getBaseObject() { return current.getBaseObject(); }
    @Override public long getBaseOffset() { return current.getBaseOffset(); }
    @Override public int getRecordLength() { return current.getRecordLength(); }
    @Override public long getKeyPrefix() { return current.getKeyPrefix(); }
  }

  /**
   * Loser (tournament) tree merge for larger run counts. {@code tree[0]} holds the overall winner
   * leaf; {@code tree[1..k-1]} hold the loser leaf at each internal node. Leaf {@code s}'s first
   * parent node is {@code (s + k) / 2}. Exhausted leaves always lose, so the next winner bubbles up
   * on a single replace-top adjustment per record. As with the linear merger, the winner is
   * advanced lazily at the start of the next {@link #loadNext()}.
   */
  private final class LoserTreeMerger extends UnsafeSorterIterator {
    private final UnsafeSorterIterator[] runs;
    private final long[] prefixes;
    private final int k;
    private final int[] tree;
    private final boolean[] exhausted;
    private long emitted = 0L;
    private boolean started = false;
    private UnsafeSorterIterator current;

    LoserTreeMerger() {
      runs = readers.toArray(new UnsafeSorterIterator[0]);
      k = runs.length;
      prefixes = new long[k];
      tree = new int[k];
      exhausted = new boolean[k];
      for (int i = 0; i < k; i++) {
        prefixes[i] = runs[i].getKeyPrefix();
        tree[i] = -1;
      }
      for (int leaf = k - 1; leaf >= 0; leaf--) {
        build(leaf);
      }
    }

    /** True if leaf {@code a} loses to leaf {@code b}; an exhausted leaf always loses. */
    private boolean loses(int a, int b) {
      if (exhausted[a]) {
        return true;
      }
      if (exhausted[b]) {
        return false;
      }
      return compareCurrent(prefixes[a], runs[a], prefixes[b], runs[b]) > 0;
    }

    /** Play a leaf up the tree during construction (internal nodes may still be empty, i.e. -1). */
    private void build(int leaf) {
      int s = leaf;
      int p = (s + k) / 2;
      while (p > 0) {
        if (tree[p] == -1) {
          tree[p] = s;
          return;
        }
        if (loses(s, tree[p])) {
          int loser = tree[p];
          tree[p] = s;
          s = loser;
        }
        p /= 2;
      }
      tree[0] = s;
    }

    /** Steady-state replay from the current winner leaf after its key changed. */
    private void replayWinner() {
      int s = tree[0];
      int p = (s + k) / 2;
      while (p > 0) {
        if (loses(s, tree[p])) {
          int loser = tree[p];
          tree[p] = s;
          s = loser;
        }
        p /= 2;
      }
      tree[0] = s;
    }

    @Override public int getNumRecords() { return (int) numRecords; }
    @Override public long getCurrentPageNumber() { throw new UnsupportedOperationException(); }
    @Override public boolean hasNext() { return emitted < numRecords; }

    @Override
    public void loadNext() throws IOException {
      if (started) {
        int winner = tree[0];
        if (runs[winner].hasNext()) {
          runs[winner].loadNext();
          prefixes[winner] = runs[winner].getKeyPrefix();
        } else {
          exhausted[winner] = true;
        }
        replayWinner();
      }
      started = true;
      current = runs[tree[0]];
      emitted++;
    }

    @Override public Object getBaseObject() { return current.getBaseObject(); }
    @Override public long getBaseOffset() { return current.getBaseOffset(); }
    @Override public int getRecordLength() { return current.getRecordLength(); }
    @Override public long getKeyPrefix() { return current.getKeyPrefix(); }
  }
}
