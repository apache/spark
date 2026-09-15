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

import java.io.IOException
import java.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.Platform

class UnsafeLoserTreeSpillMergerSuite extends SparkFunSuite {

  private val prefixComparator: PrefixComparator = new PrefixComparator {
    override def compare(a: Long, b: Long): Int = java.lang.Long.compare(a, b)
  }

  private val recordComparator: RecordComparator = new RecordComparator {
    override def compare(
        leftBase: Object, leftOff: Long, leftLen: Int,
        rightBase: Object, rightOff: Long, rightLen: Int): Int = {
      val l = Platform.getLong(leftBase, leftOff)
      val r = Platform.getLong(rightBase, rightOff)
      java.lang.Long.compare(l, r)
    }
  }

  private final class InMemoryRunIterator(records: Array[Long], prefixes: Array[Long])
    extends UnsafeSorterIterator {

    private val total = records.length
    private val recordLength = 8
    private val buffer = new Array[Byte](recordLength)
    private var index = -1

    override def getNumRecords: Int = total
    override def getCurrentPageNumber: Long = throw new UnsupportedOperationException
    override def hasNext: Boolean = index + 1 < total

    @throws[IOException]
    override def loadNext(): Unit = {
      index += 1
      Platform.putLong(buffer, Platform.BYTE_ARRAY_OFFSET, records(index))
    }

    override def getBaseObject: Object = buffer
    override def getBaseOffset: Long = Platform.BYTE_ARRAY_OFFSET
    override def getRecordLength: Int = recordLength
    override def getKeyPrefix: Long = prefixes(index)
  }

  private def newRun(records: Seq[Long], prefixes: Seq[Long]): InMemoryRunIterator = {
    require(records.length == prefixes.length)
    new InMemoryRunIterator(records.toArray, prefixes.toArray)
  }

  /** Drain to a list of (prefix, record) pairs preserving order. */
  private def drain(iter: UnsafeSorterIterator): Seq[(Long, Long)] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    while (iter.hasNext) {
      iter.loadNext()
      val v = Platform.getLong(iter.getBaseObject, iter.getBaseOffset)
      out += ((iter.getKeyPrefix, v))
    }
    out.toSeq
  }

  private def mergeWithPriorityQueue(
      runs: Seq[InMemoryRunIterator]): Seq[(Long, Long)] = {
    val merger = new UnsafeSorterSpillMerger(recordComparator, prefixComparator, runs.length)
    runs.foreach(merger.addSpillIfNotEmpty)
    drain(merger.getSortedIterator)
  }

  private def mergeWithLoserTree(
      runs: Seq[InMemoryRunIterator]): Seq[(Long, Long)] = {
    val merger = new UnsafeLoserTreeSpillMerger(recordComparator, prefixComparator, runs.length)
    runs.foreach(merger.addSpillIfNotEmpty)
    drain(merger.getSortedIterator)
  }

  /**
   * Independent oracle: flatten all runs (with run id), sort lexicographically by
   * (prefix, record, runId). Does NOT use either merger, so we can detect bugs even
   * if both PriorityQueue and LoserTree happened to share the same defect.
   */
  private def oracleSort(
      runs: Seq[(Seq[Long], Seq[Long])]): Seq[(Long, Long)] = {
    val flat = runs.zipWithIndex.flatMap { case ((records, prefixes), runId) =>
      records.zip(prefixes).map { case (rec, pre) => (pre, rec, runId) }
    }
    flat
      .sortBy { case (pre, rec, runId) => (pre, rec, runId) }
      .map { case (pre, rec, _) => (pre, rec) }
  }


  test("empty input produces empty output") {
    val merger = new UnsafeLoserTreeSpillMerger(recordComparator, prefixComparator, 0)
    val iter = merger.getSortedIterator
    assert(!iter.hasNext)
    assert(iter.getNumRecords === 0)
  }

  test("all input runs empty") {
    val runs = Seq.fill(4)(newRun(Seq.empty, Seq.empty))
    val merger = new UnsafeLoserTreeSpillMerger(recordComparator, prefixComparator, runs.length)
    runs.foreach(merger.addSpillIfNotEmpty)
    val iter = merger.getSortedIterator
    assert(!iter.hasNext)
    assert(iter.getNumRecords === 0)
  }

  test("single run passes through in order") {
    val run = newRun(Seq(1L, 5L, 7L, 9L), Seq(1L, 5L, 7L, 9L))
    val out = mergeWithLoserTree(Seq(run))
    assert(out === Seq((1L, 1L), (5L, 5L), (7L, 7L), (9L, 9L)))
  }

  test("merging two simple runs preserves global order") {
    val a = newRun(Seq(1L, 4L, 6L), Seq(1L, 4L, 6L))
    val b = newRun(Seq(2L, 3L, 5L), Seq(2L, 3L, 5L))
    val out = mergeWithLoserTree(Seq(a, b))
    assert(out.map(_._1) === Seq(1L, 2L, 3L, 4L, 5L, 6L))
  }

  test("ties broken consistently with priority queue") {
    // All prefixes equal; record ordering decides; equal records resolve by run id.
    val runs = Seq(
      newRun(Seq(10L, 20L, 30L), Seq(0L, 0L, 0L)),
      newRun(Seq(10L, 20L, 30L), Seq(0L, 0L, 0L)),
      newRun(Seq(15L, 25L), Seq(0L, 0L)))
    val pq = mergeWithPriorityQueue(runs.map(reset))
    val lt = mergeWithLoserTree(runs.map(reset))
    assert(lt === pq)
  }

  test("randomized runs match priority queue output") {
    val random = new Random(0xc0ffeeL)
    Seq(1, 2, 3, 7, 16, 33, 64).foreach { runCount =>
      Seq("low", "medium", "high").foreach { mode =>
        val (prefixRepeat, recordsPerRun) = mode match {
          case "low" => (1, 1024)
          case "medium" => (32, 1024)
          case "high" => (4096, 1024)
        }
        val runs = Array.tabulate(runCount) { runId =>
          val records = new Array[Long](recordsPerRun)
          val prefixes = new Array[Long](recordsPerRun)
          var rec = runId.toLong
          var i = 0
          while (i < recordsPerRun) {
            rec += random.nextInt(3).toLong
            prefixes(i) = (i / prefixRepeat).toLong
            records(i) = rec
            i += 1
          }
          new InMemoryRunIterator(records, prefixes)
        }.toSeq
        // Build a separate set of run iterators (same data) for the second merger.
        val twin = runs.map(reset)
        val pq = mergeWithPriorityQueue(runs)
        val lt = mergeWithLoserTree(twin)
        assert(lt.length === pq.length,
          s"length mismatch for runCount=$runCount mode=$mode")
        assert(lt === pq, s"sequence mismatch for runCount=$runCount mode=$mode")
        // Output must be globally non-decreasing on (prefix, record).
        var i = 1
        while (i < lt.length) {
          val (p0, r0) = lt(i - 1)
          val (p1, r1) = lt(i)
          assert(p0 < p1 || (p0 == p1 && r0 <= r1),
            s"out-of-order at index $i for runCount=$runCount mode=$mode")
          i += 1
        }
      }
    }
  }

  test("mix of empty and non-empty runs") {
    val runs = Seq(
      newRun(Seq.empty, Seq.empty),
      newRun(Seq(2L, 4L), Seq(0L, 0L)),
      newRun(Seq.empty, Seq.empty),
      newRun(Seq(1L, 3L, 5L), Seq(0L, 0L, 0L)),
      newRun(Seq.empty, Seq.empty))
    val out = mergeWithLoserTree(runs)
    assert(out.map(_._2) === Seq(1L, 2L, 3L, 4L, 5L))
  }

  test("non-power-of-two run counts") {
    Seq(3, 5, 6, 9, 17).foreach { count =>
      val runs = Array.tabulate(count) { i =>
        val r = (0 until 8).map(j => (i + j * count).toLong).toArray
        new InMemoryRunIterator(r, r.clone())
      }.toSeq
      val pq = mergeWithPriorityQueue(runs.map(reset))
      val lt = mergeWithLoserTree(runs.map(reset))
      assert(lt === pq, s"mismatch for runCount=$count")
    }
  }

  // -------------------------------------------------------------------------
  // Independent oracle tests: compare LoserTree directly against a
  // straight `sortBy` of the flattened input. These do NOT involve the
  // PriorityQueue merger, so they catch defects shared by both mergers.
  // -------------------------------------------------------------------------

  test("oracle: hand-written corner cases") {
    val cases: Seq[(String, Seq[(Seq[Long], Seq[Long])])] = Seq(
      ("single run, single record", Seq((Seq(7L), Seq(3L)))),
      ("single run, descending records but ascending prefixes",
        Seq((Seq(9L, 1L, 0L), Seq(1L, 2L, 3L)))),
      ("two runs, all same prefix and record (run-id breaks ties)",
        Seq(
          (Seq(5L, 5L, 5L), Seq(0L, 0L, 0L)),
          (Seq(5L, 5L, 5L), Seq(0L, 0L, 0L)))),
      ("interleaved prefix only",
        Seq(
          (Seq(0L, 0L, 0L), Seq(1L, 3L, 5L)),
          (Seq(0L, 0L, 0L), Seq(2L, 4L, 6L)))),
      ("Long.MinValue / Long.MaxValue prefixes",
        Seq(
          (Seq(0L, 0L, 0L),
            Seq(Long.MinValue, 0L, Long.MaxValue)),
          (Seq(1L, 2L),
            Seq(Long.MinValue + 1L, Long.MaxValue - 1L)))),
      ("run with one element, mixed with longer runs",
        Seq(
          (Seq(42L), Seq(10L)),
          (Seq(1L, 2L, 3L, 4L), Seq(0L, 0L, 0L, 0L)),
          (Seq(11L, 12L), Seq(20L, 20L)))),
      ("many tiny runs, one record each",
        (0 until 10).map(i => (Seq(i.toLong * 2), Seq(i.toLong)))),
      ("17 runs (non-power-of-two), varying lengths",
        (0 until 17).map { i =>
          val len = (i % 4) + 1
          val recs = (0 until len).map(_.toLong + i)
          (recs, recs)
        }))

    cases.foreach { case (name, runData) =>
      val runs = runData.map { case (recs, prefixes) =>
        newRun(recs, prefixes)
      }
      val expected = oracleSort(runData)
      val actual = mergeWithLoserTree(runs)
      assert(actual === expected, s"oracle mismatch for case [$name]")
      assert(actual.length === expected.length, s"length mismatch for case [$name]")
    }
  }

  test("oracle: randomized fuzz against direct sort (no PQ involved)") {
    val rng = new Random(0xdeadbeefL)
    var iteration = 0
    while (iteration < 200) {
      val runCount = rng.nextInt(20) + 1 // 1..20 runs (covers 1, non-pow-2, pow-2)
      val runData = (0 until runCount).map { _ =>
        val len = rng.nextInt(50) // some runs may be empty
        // Build a sorted run on (prefix, record).
        val pairs = (0 until len).map { _ =>
          val pre = rng.nextInt(8).toLong // small domain -> heavy ties
          val rec = rng.nextInt(8).toLong
          (pre, rec)
        }.sortBy { case (p, r) => (p, r) }
        (pairs.map(_._2), pairs.map(_._1))
      }
      val expected = oracleSort(runData)
      val runs = runData.map { case (recs, pres) => newRun(recs, pres) }
      val actual = mergeWithLoserTree(runs)
      assert(actual.length === expected.length,
        s"length mismatch on iteration=$iteration runCount=$runCount " +
          s"expected=${expected.length} actual=${actual.length}")
      assert(actual === expected,
        s"sequence mismatch on iteration=$iteration runCount=$runCount")
      iteration += 1
    }
  }

  test("getNumRecords is exact and hasNext drains all and only those records") {
    val runData = Seq(
      (Seq(1L, 2L, 3L), Seq(0L, 0L, 0L)),
      (Seq(4L, 5L), Seq(1L, 2L)),
      (Seq.empty[Long], Seq.empty[Long]),
      (Seq(6L), Seq(3L)))
    val expectedTotal = runData.map(_._1.length).sum

    val merger = new UnsafeLoserTreeSpillMerger(
      recordComparator, prefixComparator, runData.length)
    runData.foreach { case (recs, pres) => merger.addSpillIfNotEmpty(newRun(recs, pres)) }
    val it = merger.getSortedIterator
    assert(it.getNumRecords === expectedTotal)
    var consumed = 0
    while (it.hasNext) {
      it.loadNext()
      consumed += 1
    }
    assert(consumed === expectedTotal)
    assert(!it.hasNext)
  }

  test("output is globally non-decreasing for randomized input (oracle-free invariant)") {
    val rng = new Random(0x5eedL)
    (0 until 50).foreach { _ =>
      val runCount = rng.nextInt(16) + 1
      val runs = (0 until runCount).map { _ =>
        val len = rng.nextInt(40)
        val pairs = (0 until len).map { _ =>
          (rng.nextInt(6).toLong, rng.nextInt(6).toLong)
        }.sortBy { case (p, r) => (p, r) }
        newRun(pairs.map(_._2), pairs.map(_._1))
      }
      val merged = mergeWithLoserTree(runs)
      var i = 1
      while (i < merged.length) {
        val (p0, r0) = merged(i - 1)
        val (p1, r1) = merged(i)
        assert(p0 < p1 || (p0 == p1 && r0 <= r1),
          s"non-decreasing invariant violated at $i: ${(p0, r0)} > ${(p1, r1)}")
        i += 1
      }
    }
  }

  /** Rebuild a fresh iterator from the same backing data so each merger gets a clean cursor. */
  private def reset(it: InMemoryRunIterator): InMemoryRunIterator = {
    val recordsField = classOf[InMemoryRunIterator].getDeclaredField("records")
    recordsField.setAccessible(true)
    val prefixesField = classOf[InMemoryRunIterator].getDeclaredField("prefixes")
    prefixesField.setAccessible(true)
    new InMemoryRunIterator(
      recordsField.get(it).asInstanceOf[Array[Long]],
      prefixesField.get(it).asInstanceOf[Array[Long]])
  }
}
