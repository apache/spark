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

package org.apache.spark.util.kvstore

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.commons.io.FileUtils

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark suite for the KVStore implemented based on RocksDB.
 *
 * The benchmarks are run over two different types (one with just a natural index, and one
 * with a ref index), over a set of elements, and the following tests are performed:
 *
 * - write (then update) elements in sequential natural key order
 * - write (then update) elements in random natural key order
 * - iterate over natural index, ascending and descending
 * - iterate over ref index, ascending and descending
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RocksDBBenchmark-results.txt".
 * }}}
 */
object RocksDBBenchmark extends BenchmarkBase {

  private val COUNT = 1024
  private val ITERATIONS = 4
  private val IDGEN = new AtomicInteger()

  private var db: RocksDB = _
  private var dbpath: File = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("RocksDB Database Lifecycle Benchmark") {
      databaseLifecycleBenchmark()
    }

    runBenchmark("RocksDB Sequential Operations Benchmark") {
      sequentialOperationsBenchmark()
    }

    runBenchmark("RocksDB Random Operations Benchmark") {
      randomOperationsBenchmark()
    }

    runBenchmark("RocksDB Iteration Benchmark") {
      iterationBenchmark()
    }
  }

  private def databaseLifecycleBenchmark(): Unit = {
    val benchmark =
      new Benchmark("Database Lifecycle Operations", 1, ITERATIONS, output = output)

    benchmark.addTimerCase("Database Creation") { timer =>
      try {
        timer.startTiming()
        setupDB()
        timer.stopTiming()
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("Database Close") { timer =>
      try {
        setupDB()
        val entries = createSimpleType().take(10)
        // Add some data to make close operation more realistic
        entries.foreach(db.write)
      } finally {
        timer.startTiming()
        db.close()
        timer.stopTiming()
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def sequentialOperationsBenchmark(): Unit = {
    val benchmark = new Benchmark("Sequential Operations", COUNT, ITERATIONS, output = output)

    benchmark.addCase("Sequential Writes (No Index)") { _ =>
      setupDB()
      try {
        val entries = createSimpleType()
        entries.foreach(db.write)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Sequential Updates (No Index)") { _ =>
      setupDB()
      try {
        val entries = createSimpleType()
        // First write
        entries.foreach(db.write)
        // Then update
        entries.foreach { entry =>
          entry.name = s"updated_${entry.name}"
          db.write(entry)
        }
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Sequential Deletes (No Index)") { _ =>
      setupDB()
      try {
        val entries = createSimpleType()
        // First write
        entries.foreach(db.write)
        // Then delete
        entries.foreach(entry => db.delete(entry.getClass, entry.key))
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Sequential Writes (Indexed)") { _ =>
      setupDB()
      try {
        val entries = createIndexedType()
        entries.foreach(db.write)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Sequential Updates (Indexed)") { _ =>
      setupDB()
      try {
        val entries = createIndexedType()
        // First write
        entries.foreach(db.write)
        // Then update
        entries.foreach { entry =>
          entry.name = s"updated_${entry.name}"
          db.write(entry)
        }
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Sequential Deletes (Indexed)") { _ =>
      setupDB()
      try {
        val entries = createIndexedType()
        // First write
        entries.foreach(db.write)
        // Then delete
        entries.foreach(entry => db.delete(entry.getClass, entry.key))
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def randomOperationsBenchmark(): Unit = {
    val benchmark = new Benchmark("Random Operations", COUNT, ITERATIONS, output = output)

    benchmark.addCase("Random Writes (No Index)") { _ =>
      setupDB()
      try {
        val entries = Random.shuffle(createSimpleType())
        entries.foreach(db.write)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Random Updates (No Index)") { _ =>
      setupDB()
      try {
        val entries = createSimpleType()
        // First write
        entries.foreach(db.write)
        // Shuffle and update
        val shuffled = Random.shuffle(entries)
        shuffled.foreach { entry =>
          entry.name = s"updated_${entry.name}"
          db.write(entry)
        }
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Random Deletes (No Index)") { _ =>
      setupDB()
      try {
        val entries = createSimpleType()
        // First write
        entries.foreach(db.write)
        // Shuffle and delete
        val shuffled = Random.shuffle(entries)
        shuffled.foreach(entry => db.delete(entry.getClass, entry.key))
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Random Writes (Indexed)") { _ =>
      setupDB()
      try {
        val entries = Random.shuffle(createIndexedType())
        entries.foreach(db.write)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Random Updates (Indexed)") { _ =>
      setupDB()
      try {
        val entries = createIndexedType()
        // First write
        entries.foreach(db.write)
        // Shuffle and update
        val shuffled = Random.shuffle(entries)
        shuffled.foreach { entry =>
          entry.name = s"updated_${entry.name}"
          db.write(entry)
        }
      } finally {
        cleanupDB()
      }
    }

    benchmark.addCase("Random Deletes (Indexed)") { _ =>
      setupDB()
      try {
        val entries = createIndexedType()
        // First write
        entries.foreach(db.write)
        // Shuffle and delete
        val shuffled = Random.shuffle(entries)
        shuffled.foreach(entry => db.delete(entry.getClass, entry.key))
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def iterationBenchmark(): Unit = {
    val benchmark = new Benchmark("Iteration Operations", COUNT, ITERATIONS, output = output)

    // Setup data for iteration benchmarks
    setupDB()
    val entries = Random.shuffle(createIndexedType())
    try {
      entries.foreach(db.write)

      val view = db.view(classOf[IndexedType])

      benchmark.addCase("Natural Index Ascending") { _ =>
        val it = view.closeableIterator()
        try {
          while (it.hasNext) {
            it.next()
          }
        } finally {
          it.close()
        }
      }

      benchmark.addCase("Natural Index Descending") { _ =>
        val it = view.reverse().closeableIterator()
        try {
          while (it.hasNext) {
            it.next()
          }
        } finally {
          it.close()
        }
      }

      benchmark.addCase("Reference Index Ascending") { _ =>
        val it = view.index("name").closeableIterator()
        try {
          while (it.hasNext) {
            it.next()
          }
        } finally {
          it.close()
        }
      }

      benchmark.addCase("Reference Index Descending") { _ =>
        val it = view.index("name").reverse().closeableIterator()
        try {
          while (it.hasNext) {
            it.next()
          }
        } finally {
          it.close()
        }
      }

      benchmark.run()
    } finally {
      cleanupDB()
    }
  }

  private def setupDB(): Unit = {
    try {
      dbpath = File.createTempFile("test.", ".rdb")
      dbpath.delete()
      db = new RocksDB(dbpath)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to setup database", e)
    }
  }

  private def cleanupDB(): Unit = {
    if (db != null) {
      try {
        db.close()
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
      db = null
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath)
      dbpath = null
    }
  }

  private def createSimpleType(): Seq[SimpleType] = {
    val entries = ArrayBuffer[SimpleType]()
    for (_ <- 0 until COUNT) {
      val t = new SimpleType()
      t.key = IDGEN.getAndIncrement()
      t.name = s"name${t.key % 1024}"
      entries += t
    }
    entries.toSeq
  }

  private def createIndexedType(): Seq[IndexedType] = {
    val entries = ArrayBuffer[IndexedType]()
    for (_ <- 0 until COUNT) {
      val t = new IndexedType()
      t.key = IDGEN.getAndIncrement()
      t.name = s"name${t.key % 1024}"
      entries += t
    }
    entries.toSeq
  }
}
