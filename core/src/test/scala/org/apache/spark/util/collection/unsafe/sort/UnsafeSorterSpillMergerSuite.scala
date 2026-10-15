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

package org.apache.spark.util.collection.unsafe.sort

import java.util.NoSuchElementException

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.Platform

/**
 * Unit tests for the merge iterators returned by [[UnsafeSorterSpillMerger.getSortedIterator]]:
 * the empty merger (no runs), the linear min-scan merger (at most
 * {@code LINEAR_MERGE_MAX_RUNS} runs), and the loser-tree merger (more than that). The merger is
 * fed in-memory sorted runs and its emitted stream is compared against the globally sorted input;
 * every case is run for run counts spanning the linear/loser-tree boundary so both structures are
 * exercised. Ordering is by 8-byte key prefix, then the record comparator on a prefix tie.
 */
class UnsafeSorterSpillMergerSuite extends SparkFunSuite {

  // The most runs the linear scan merges; one more switches to the loser tree.
  private val maxLinearRuns = UnsafeSorterSpillMerger.LINEAR_MERGE_MAX_RUNS

  // Run counts spanning the linear/loser-tree boundary.
  private val runCounts =
    (Seq(1, 2, 16, 64) ++ Seq(maxLinearRuns - 1, maxLinearRuns, maxLinearRuns + 1)).distinct.sorted

  // Orders records as signed 8-byte longs; the merger uses this to break prefix ties.
  private val recordComparator = new RecordComparator {
    override def compare(
        leftBase: AnyRef, leftOff: Long, leftLen: Int,
        rightBase: AnyRef, rightOff: Long, rightLen: Int): Int = {
      java.lang.Long.compare(
        Platform.getLong(leftBase, leftOff), Platform.getLong(rightBase, rightOff))
    }
  }

  // The total order the merger must produce: prefix first (signed long), then record on a tie.
  private val expectedOrder: Ordering[(Long, Long)] = Ordering.by(e => (e._1, e._2))

  /** A single-use iterator over one sorted run of (prefix, record) pairs, records in a long[]. */
  private final class ArrayRun(entries: Seq[(Long, Long)]) extends UnsafeSorterIterator {
    private val prefixes = entries.map(_._1).toArray
    private val records = entries.map(_._2).toArray
    private var pos = -1
    override def hasNext: Boolean = pos < records.length - 1
    override def loadNext(): Unit = pos += 1
    override def getBaseObject: AnyRef = records
    override def getBaseOffset: Long = Platform.LONG_ARRAY_OFFSET + pos.toLong * 8L
    override def getRecordLength: Int = 8
    override def getKeyPrefix: Long = prefixes(pos)
    override def getNumRecords: Int = records.length
    override def getCurrentPageNumber: Long = throw new UnsupportedOperationException()
  }

  /** Merges the runs and returns the emitted (prefix, record) pairs in emission order. */
  private def merge(runs: Seq[Seq[(Long, Long)]]): Seq[(Long, Long)] =
    mergeIterators(runs.map(new ArrayRun(_)))

  /** Merges the given run iterators and returns the emitted pairs in emission order. */
  private def mergeIterators(runs: Seq[UnsafeSorterIterator]): Seq[(Long, Long)] = {
    val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, runs.size)
    runs.foreach(merger.addSpillIfNotEmpty)
    val it = merger.getSortedIterator()
    val out = ArrayBuffer.empty[(Long, Long)]
    while (it.hasNext) {
      it.loadNext()
      assert(it.getRecordLength === 8)
      out += ((it.getKeyPrefix, Platform.getLong(it.getBaseObject, it.getBaseOffset)))
    }
    out.toSeq
  }

  /** Asserts the merge of `runs` is the globally sorted concatenation of the (sorted) inputs. */
  private def checkMerge(runs: Seq[Seq[(Long, Long)]]): Unit = {
    runs.foreach(r => assert(r == r.sorted(expectedOrder), s"run not pre-sorted: $r"))
    val expected = runs.flatten.sorted(expectedOrder)
    val actual = merge(runs)
    assert(actual.size === expected.size)
    assert(actual === expected)
  }

  // Splits `total` records across `k` runs of uneven (occasionally empty) length, each sorted by
  // (prefix, record). `prefixOf` maps a record to its prefix, so a coarse mapping produces frequent
  // prefix ties that force the record comparator.
  private def buildRuns(k: Int, total: Int, prefixOf: Long => Long, seed: Long)
    : Seq[Seq[(Long, Long)]] = {
    val rand = new Random(seed)
    // Random split points give runs of differing sizes, including possible empties.
    val cuts = Array.fill(k - 1)(rand.nextInt(total + 1)).sorted
    val bounds = (0 +: cuts.toSeq) :+ total
    bounds.sliding(2).map { case Seq(lo, hi) =>
      Seq.fill(hi - lo)(rand.nextLong()).map(r => (prefixOf(r), r)).sortBy(e => (e._1, e._2))
    }.toSeq
  }

  // k non-empty runs of n records each. Run j holds j, j + k, j + 2k, ..., so the runs interleave
  // and the merge switches runs on every record.
  private def interleavedRuns(k: Int, n: Int): Seq[Seq[(Long, Long)]] =
    (0 until k).map { j => (0 until n).map { i => val v = (i * k + j).toLong; (v, v) } }

  test("empty merger: no runs yields an empty, zero-length stream") {
    val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, 0)
    val it = merger.getSortedIterator()
    assert(!it.hasNext)
    assert(it.getNumRecords === 0)
    intercept[NoSuchElementException](it.loadNext())
  }

  test("empty merger: exhausted (empty) runs are skipped, not merged") {
    // addSpillIfNotEmpty drops readers with no records, so a merger fed only empty runs is empty.
    val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, 3)
    (0 until 3).foreach(_ => merger.addSpillIfNotEmpty(new ArrayRun(Seq.empty)))
    val it = merger.getSortedIterator()
    assert(!it.hasNext)
    assert(it.getNumRecords === 0)
  }

  test("linear merger: a single run passes through unchanged") {
    val run = Seq(1L, 2L, 5L, 5L, 9L).map(r => (r, r))
    assert(merge(Seq(run)) === run)
  }

  test("prefix ordering takes precedence over the record comparator") {
    // Record 5 sorts before 100 by value, but prefix 1 < prefix 2 must order (1, 100) first.
    val runA = Seq((1L, 100L))
    val runB = Seq((2L, 5L))
    assert(merge(Seq(runA, runB)) === Seq((1L, 100L), (2L, 5L)))
  }

  test("the record comparator breaks prefix ties") {
    // All prefixes equal, so ordering is decided entirely by the record comparator.
    val runA = Seq((7L, 30L), (7L, 40L))
    val runB = Seq((7L, 10L), (7L, 20L), (7L, 50L))
    assert(merge(Seq(runA, runB)) ===
      Seq((7L, 10L), (7L, 20L), (7L, 30L), (7L, 40L), (7L, 50L)))
  }

  test("prefixes are ordered as signed longs (min/max boundaries)") {
    val run = Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue).map(r => (r, r))
    val runs = run.map(Seq(_)) // one boundary value per run
    checkMerge(runs)
    assert(merge(runs).map(_._1) === Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue))
  }

  for (k <- runCounts) {
    test(s"merge with distinct prefixes preserves global order (k=$k)") {
      // prefix == record: comparisons resolve on the prefix, never the record comparator.
      checkMerge(buildRuns(k, 500, r => r, seed = 100L + k))
    }

    test(s"merge with heavy prefix ties preserves global order (k=$k)") {
      // Few distinct prefixes: the record comparator breaks ties on nearly every step.
      checkMerge(buildRuns(k, 500, r => r & 0x7L, seed = 200L + k))
    }

    test(s"merge with all-equal prefixes preserves global order (k=$k)") {
      // A degenerate single-prefix case: every comparison falls to the record comparator.
      checkMerge(buildRuns(k, 300, _ => 42L, seed = 300L + k))
    }
  }

  // Pin the linear/loser-tree boundary. buildRuns can produce empty runs, which the merger drops,
  // and so pull a count just above the boundary below it. Here all k runs are non-empty, so the
  // intended structure (linear at maxLinearRuns, loser tree one above) actually runs.
  for (k <- Seq(maxLinearRuns, maxLinearRuns + 1)) {
    test(s"boundary: exactly $k non-empty runs merge in global order") {
      val runs = interleavedRuns(k, 20)
      assert(runs.forall(_.nonEmpty) && runs.size == k)
      checkMerge(runs)
    }
  }

  test("a run advanced before it is added contributes only its remaining records") {
    // getNumRecords() is the total a run was created over, including records it was advanced
    // past, so the merge must stop when its runs are exhausted rather than after that many.
    for (k <- Seq(2, maxLinearRuns + 1)) {
      val runs = interleavedRuns(k, 5)
      val first = new ArrayRun(runs.head)
      first.loadNext()
      val expected = (runs.head.tail +: runs.tail).flatten.sorted(expectedOrder)
      assert(mergeIterators(first +: runs.tail.map(new ArrayRun(_))) === expected)
    }
  }

  test("loadNext past the end throws NoSuchElementException") {
    for (k <- Seq(2, maxLinearRuns + 1)) {
      val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, k)
      interleavedRuns(k, 3).foreach(r => merger.addSpillIfNotEmpty(new ArrayRun(r)))
      val it = merger.getSortedIterator()
      while (it.hasNext) {
        it.loadNext()
      }
      intercept[NoSuchElementException](it.loadNext())
    }
  }

  test("getSortedIterator can only be called once") {
    val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, 1)
    merger.addSpillIfNotEmpty(new ArrayRun(Seq((1L, 1L))))
    merger.getSortedIterator()
    val e = intercept[IllegalStateException](merger.getSortedIterator())
    assert(e.getMessage.contains("only be called once"))
  }

  test("getNumRecords returns the total across runs for both merge structures") {
    // Linear (k=4) and loser-tree (k=32) both report the sum of their runs' record counts.
    for (k <- Seq(4, 32)) {
      val runs = buildRuns(k, 400, r => r, seed = 400L + k)
      val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, k)
      runs.foreach(r => merger.addSpillIfNotEmpty(new ArrayRun(r)))
      assert(merger.getSortedIterator().getNumRecords === runs.map(_.size).sum)
    }
  }
}
