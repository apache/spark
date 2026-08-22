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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.Platform

/**
 * Benchmark comparing UnsafeSorterSpillMerger (priority queue) vs
 * UnsafeLoserTreeSpillMerger (loser tree) for k-way merge of spill iterators.
 *
 * To run:
 * {{{
 *   build/sbt "core/Test/runMain
 *     org.apache.spark.util.collection.unsafe.sort.SpillMergerBenchmark"
 * }}}
 */
object SpillMergerBenchmark extends BenchmarkBase {

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

  /**
   * In-memory UnsafeSorterIterator backed by pre-built parallel arrays of (record, prefix).
   * Each record holds a single long (8 bytes) so the record comparator has real work to do
   * when prefixes tie.
   */
  private final class InMemoryRunIterator(records: Array[Long], prefixes: Array[Long])
    extends UnsafeSorterIterator {

    val sourceRecords: Array[Long] = records
    val sourcePrefixes: Array[Long] = prefixes

    private val total = records.length
    private val recordLength = 8
    private val buffer = new Array[Byte](recordLength)
    private var index = -1

    override def getNumRecords: Int = total

    override def getCurrentPageNumber: Long =
      throw new UnsupportedOperationException

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

  private final case class Scenario(
      name: String,
      prefixRepeat: Int,
      duplicateFactor: Int)

  private def buildRuns(
      runCount: Int,
      recordsPerRun: Int,
      scenario: Scenario): Array[InMemoryRunIterator] = {
    val random = new Random(42L)
    Array.tabulate(runCount) { runId =>
      val records = new Array[Long](recordsPerRun)
      val prefixes = new Array[Long](recordsPerRun)
      var suffix = runId.toLong
      var i = 0
      while (i < recordsPerRun) {
        val prefix = (i / scenario.prefixRepeat).toLong
        val suffixStep = if (i % scenario.duplicateFactor == 0) 1 else 0
        suffix += suffixStep + random.nextInt(3)
        prefixes(i) = prefix
        records(i) = suffix
        i += 1
      }
      new InMemoryRunIterator(records, prefixes)
    }
  }

  private def cloneRuns(runs: Array[InMemoryRunIterator]): Array[InMemoryRunIterator] = {
    runs.map(r => new InMemoryRunIterator(r.sourceRecords, r.sourcePrefixes))
  }

  private def consumePriorityQueue(runs: Array[InMemoryRunIterator]): Long = {
    val merger = new UnsafeSorterSpillMerger(recordComparator, prefixComparator, runs.length)
    runs.foreach(merger.addSpillIfNotEmpty)
    drain(merger.getSortedIterator)
  }

  private def consumeLoserTree(runs: Array[InMemoryRunIterator]): Long = {
    val merger = new UnsafeLoserTreeSpillMerger(recordComparator, prefixComparator, runs.length)
    runs.foreach(merger.addSpillIfNotEmpty)
    drain(merger.getSortedIterator)
  }

  private def drain(iter: UnsafeSorterIterator): Long = {
    var checksum = 0L
    while (iter.hasNext) {
      iter.loadNext()
      val v = Platform.getLong(iter.getBaseObject, iter.getBaseOffset)
      checksum = checksum * 31 + v + java.lang.Long.rotateLeft(iter.getKeyPrefix, 17)
    }
    checksum
  }

  private def benchmarkCase(
      runCount: Int,
      recordsPerRun: Int,
      scenario: Scenario): Unit = {
    val runs = buildRuns(runCount, recordsPerRun, scenario)
    val expected = consumePriorityQueue(cloneRuns(runs))
    val actual = consumeLoserTree(cloneRuns(runs))
    require(expected == actual, s"checksum mismatch: $expected != $actual")

    val benchmark = new Benchmark(
      s"k-way merge scenario=${scenario.name} runs=$runCount " +
        s"recordsPerRun=$recordsPerRun",
      runCount.toLong * recordsPerRun,
      output = output)

    benchmark.addCase("priority-queue") { _ =>
      val checksum = consumePriorityQueue(cloneRuns(runs))
      if (checksum != expected) {
        throw new IllegalStateException(s"unexpected checksum $checksum")
      }
    }

    benchmark.addCase("loser-tree") { _ =>
      val checksum = consumeLoserTree(cloneRuns(runs))
      if (checksum != expected) {
        throw new IllegalStateException(s"unexpected checksum $checksum")
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val recordsPerRun = 500000
    val runCounts = Seq(4, 8, 16, 64, 256)
    val scenarios = Seq(
      Scenario("low-prefix-collision", prefixRepeat = 1, duplicateFactor = 1),
      Scenario("medium-prefix-collision", prefixRepeat = 64, duplicateFactor = 8),
      Scenario("high-prefix-collision", prefixRepeat = 4096, duplicateFactor = 8))

    runBenchmark("Spill merger: PriorityQueue vs LoserTree") {
      scenarios.foreach { scenario =>
        runCounts.foreach { runCount =>
          benchmarkCase(runCount, recordsPerRun, scenario)
        }
      }
    }
  }
}
