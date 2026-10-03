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

import java.util.{Comparator, PriorityQueue}

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.Platform

/**
 * Benchmark for the k-way merge in [[UnsafeSorterSpillMerger]] (the external-sort spill merge).
 * It isolates merge CPU: the runs are in-memory sorted arrays, not on-disk spill readers, so
 * there is no read/decode I/O -- the numbers are an upper bound on what the merge structure can
 * contribute end-to-end. Each run is a sorted array of 8-byte records; the key prefix drives the
 * comparison, and the record comparator breaks prefix ties. Two prefix regimes are measured:
 * distinct prefixes (prefix-only comparisons) and heavily duplicated prefixes (the record
 * comparator is exercised on nearly every step).
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/UnsafeSorterSpillMergerBenchmark-results.txt".
 * }}}
 */
object UnsafeSorterSpillMergerBenchmark extends BenchmarkBase {

  private val numRecords = 1 << 22 // ~4.19M records merged per run, held constant across k
  // Points on both sides of LINEAR_MERGE_MAX_RUNS = 8 (9, 12) show the linear/loser-tree crossover.
  private val runCounts = Seq(2, 4, 8, 9, 12, 16, 64)

  // Compares two records as signed 8-byte longs; used to break prefix ties.
  private val recordComparator = new RecordComparator {
    override def compare(
        leftBase: AnyRef, leftOff: Long, leftLen: Int,
        rightBase: AnyRef, rightOff: Long, rightLen: Int): Int = {
      java.lang.Long.compare(
        Platform.getLong(leftBase, leftOff), Platform.getLong(rightBase, rightOff))
    }
  }

  /** A UnsafeSorterIterator over a run's sorted records, held in a long[] (8 bytes each). */
  private final class ArrayRun(prefixes: Array[Long], records: Array[Long])
    extends UnsafeSorterIterator {
    private var pos: Int = -1
    override def hasNext: Boolean = pos < prefixes.length - 1
    override def loadNext(): Unit = pos += 1
    override def getBaseObject: AnyRef = records
    override def getBaseOffset: Long = Platform.LONG_ARRAY_OFFSET + pos.toLong * 8L
    override def getRecordLength: Int = 8
    override def getKeyPrefix: Long = prefixes(pos)
    override def getNumRecords: Int = prefixes.length
    override def getCurrentPageNumber: Long = throw new UnsupportedOperationException()
  }

  // Precomputes k sorted runs totaling numRecords as (prefixes, records) backing arrays. Built
  // once, outside the timed region. prefixOf maps a record to its key prefix, so a coarse mapping
  // (few distinct prefixes) forces the record comparator on ties.
  private def buildRunData(k: Int, prefixOf: Long => Long): Array[(Array[Long], Array[Long])] = {
    val data = new Array[(Array[Long], Array[Long])](k)
    val base = numRecords / k
    var built = 0
    for (j <- 0 until k) {
      val size = if (j < numRecords - base * k) base + 1 else base
      val rand = new Random(1234L + j)
      val records = Array.fill(size)(rand.nextLong()).sortBy(r => (prefixOf(r), r))
      data(j) = (records.map(prefixOf), records)
      built += size
    }
    assert(built == numRecords)
    data
  }

  // Wraps the shared backing arrays in fresh single-use iterators for one merge invocation.
  private def freshRuns(data: Array[(Array[Long], Array[Long])]): Array[ArrayRun] =
    data.map { case (prefixes, records) => new ArrayRun(prefixes, records) }

  private var sink: Long = 0L

  private def mergeOnce(runs: Array[ArrayRun]): Unit = {
    val merger = new UnsafeSorterSpillMerger(recordComparator, PrefixComparators.LONG, runs.length)
    runs.foreach(merger.addSpillIfNotEmpty)
    val it = merger.getSortedIterator()
    var checksum = 0L
    while (it.hasNext) {
      it.loadNext()
      checksum += it.getKeyPrefix
    }
    sink += checksum
  }

  // Baseline: the pre-PR PriorityQueue merge (poll + add == two sift passes per record). Kept in
  // the benchmark so the loser-tree / linear speedup is reproducible from this file alone.
  private def mergeOnceHeap(runs: Array[ArrayRun]): Unit = {
    val comparator = new Comparator[UnsafeSorterIterator] {
      override def compare(left: UnsafeSorterIterator, right: UnsafeSorterIterator): Int = {
        val p = PrefixComparators.LONG.compare(left.getKeyPrefix, right.getKeyPrefix)
        if (p != 0) p
        else recordComparator.compare(
          left.getBaseObject, left.getBaseOffset, left.getRecordLength,
          right.getBaseObject, right.getBaseOffset, right.getRecordLength)
      }
    }
    val queue = new PriorityQueue[UnsafeSorterIterator](runs.length, comparator)
    runs.foreach { r => if (r.hasNext) { r.loadNext(); queue.add(r) } }
    var checksum = 0L
    while (!queue.isEmpty) {
      val top = queue.poll()
      checksum += top.getKeyPrefix
      if (top.hasNext) { top.loadNext(); queue.add(top) }
    }
    sink += checksum
  }

  private def runRegime(name: String, prefixOf: Long => Long): Unit = {
    val benchmark = new Benchmark(name, numRecords, output = output)
    for (k <- runCounts) {
      val data = buildRunData(k, prefixOf) // built once here, before timing
      // Fresh iterators each invocation (single-use). Baseline first, then the new merge, so the
      // per-row times for each k are directly comparable.
      benchmark.addCase(s"PriorityQueue k=$k") { _ =>
        mergeOnceHeap(freshRuns(data))
      }
      benchmark.addCase(s"merge k=$k") { _ =>
        mergeOnce(freshRuns(data))
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("UnsafeSorterSpillMerger k-way merge") {
      // Distinct prefixes: comparisons resolve on the cached prefix, never the record comparator.
      runRegime(s"distinct prefixes, $numRecords records", r => r)
      // Duplicated prefixes (256 distinct values): the record comparator breaks ties constantly.
      runRegime(s"duplicate prefixes, $numRecords records", r => r & 0xFFL)
    }
  }
}
