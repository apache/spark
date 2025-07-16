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

import scala.beans.BeanProperty
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
 * - create and close rocksdb
 * - write/update/delete elements in sequential natural key order
 * - write/update/delete elements in random natural key order
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
    runBenchmark("RocksDB Lifecycle Benchmark") {
      databaseLifecycle()
    }

    runBenchmark("Sequential Operations Benchmark") {
      sequentialWrites()
      sequentialUpdates()
      sequentialDeletes()
    }

    runBenchmark("Random Operations Benchmark") {
      randomWrites()
      randomUpdates()
      randomDeletes()
    }

    runBenchmark("Natural Index Benchmark") {
      naturalIndexCreateIterator()
      naturalIndexIteration()
    }

    runBenchmark("Ref Index Benchmark") {
      refIndexCreateIterator()
      refIndexIteration()
    }
  }

  private def databaseLifecycle(): Unit = {
    val benchmark =
      new Benchmark("RocksDB Lifecycle Operations", 1, ITERATIONS, output = output)

    benchmark.addTimerCase("DB Creation") { timer =>
      try {
        dbpath = File.createTempFile("test.", ".rdb")
        dbpath.delete()
        timer.startTiming()
        db = new RocksDB(dbpath)
        timer.stopTiming()
      } catch {
        case e: Exception =>
          throw new RuntimeException("Failed to setup database", e)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("DB Close") { timer =>
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

  private def sequentialDeletes(): Unit = {
    val benchmark = new Benchmark("Sequential Deletes", COUNT, ITERATIONS, output = output)

    benchmark.addTimerCase("Indexed") { timer =>
      try {
        setupDB()
        val entries = createIndexedType()
        entries.foreach(db.write)
        indexDelete(entries, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("No Index") { timer =>
      try {
        setupDB()
        val entries = createSimpleType()
        entries.foreach(db.write)
        noIndexDelete(entries, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def sequentialUpdates(): Unit = {
    val benchmark = new Benchmark("Sequential Updates", COUNT, ITERATIONS, output = output)

    benchmark.addTimerCase("Indexed") { timer =>
      try {
        setupDB()
        val entries = createIndexedType()
        entries.foreach(db.write)
        indexUpdate(entries, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("No Index") { timer =>
      try {
        setupDB()
        val entries = createSimpleType()
        entries.foreach(db.write)
        noIndexUpdate(entries, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }


  private def sequentialWrites(): Unit = {
    val benchmark = new Benchmark("Sequential Writes", COUNT, ITERATIONS, output = output)

    benchmark.addTimerCase("Indexed") { timer =>
      try {
        setupDB()
        val entries = createIndexedType()
        indexWrite(entries, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("No Index") { timer =>
      try {
        setupDB()
        val entries = createSimpleType()
        noIndexWrite(entries, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def randomDeletes(): Unit = {
    val benchmark = new Benchmark("Random Deletes", COUNT, ITERATIONS, output = output)

    benchmark.addTimerCase("Indexed") { timer =>
      try {
        setupDB()
        val entries = createIndexedType()
        entries.foreach(db.write)
        val shuffled = Random.shuffle(entries)
        indexDelete(shuffled, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("No Index") { timer =>
      try {
        setupDB()
        val entries = createSimpleType()
        entries.foreach(db.write)
        val shuffled = Random.shuffle(entries)
        noIndexDelete(shuffled, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def randomUpdates(): Unit = {
    val benchmark = new Benchmark("Random Updates", COUNT, ITERATIONS, output = output)

    benchmark.addTimerCase("Indexed") { timer =>
      try {
        setupDB()
        val entries = createIndexedType()
        entries.foreach(db.write)
        val shuffled = Random.shuffle(entries)
        indexUpdate(shuffled, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("No Index") { timer =>
      try {
        setupDB()
        val entries = createSimpleType()
        entries.foreach(db.write)
        val shuffled = Random.shuffle(entries)
        noIndexUpdate(shuffled, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def randomWrites(): Unit = {
    val benchmark = new Benchmark("Random Writes", COUNT, ITERATIONS, output = output)

    benchmark.addTimerCase("Indexed") { timer =>
      try {
        setupDB()
        val shuffled = Random.shuffle(createIndexedType())
        indexWrite(shuffled, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.addTimerCase("No Index") { timer =>
      try {
        setupDB()
        val shuffled = Random.shuffle(createSimpleType())
        noIndexWrite(shuffled, timer)
      } finally {
        cleanupDB()
      }
    }

    benchmark.run()
  }

  private def naturalIndexCreateIterator(): Unit = {
    val benchmark =
      new Benchmark("Natural Index - Create Iterator", COUNT, ITERATIONS, output = output)

    try {
      setupDB()
      val entries = Random.shuffle(createIndexedType())
      entries.foreach(db.write)

      val view = db.view(classOf[IndexedType])

      benchmark.addTimerCase("Ascending") { timer =>
        timer.startTiming()
        val it = view.closeableIterator()
        timer.stopTiming()
        it.close()
      }

      benchmark.addTimerCase("Descending") { timer =>
        timer.startTiming()
        val it = view.reverse().closeableIterator()
        timer.stopTiming()
        it.close()
      }

      benchmark.run()
    } finally {
      cleanupDB()
    }
  }

  private def naturalIndexIteration(): Unit = {
    val benchmark = new Benchmark("Natural Index - Iteration", COUNT, ITERATIONS, output = output)

    try {
      setupDB()
      val entries = Random.shuffle(createIndexedType())
      entries.foreach(db.write)

      val view = db.view(classOf[IndexedType])

      benchmark.addTimerCase("Ascending") { timer =>
        val it = view.closeableIterator()
        try {
          timer.startTiming()
          while (it.hasNext) {
            it.next()
          }
          timer.stopTiming()
        } finally {
          it.close()
        }
      }

      benchmark.addTimerCase("Descending") { timer =>
        val it = view.reverse().closeableIterator()
        try {
          timer.startTiming()
          while (it.hasNext) {
            it.next()
          }
          timer.stopTiming()
        } finally {
          it.close()
        }
      }

      benchmark.run()
    } finally {
      cleanupDB()
    }
  }

  private def refIndexCreateIterator(): Unit = {
    val benchmark = new Benchmark("Ref Index - Create Iterator", COUNT, ITERATIONS, output = output)

    try {
      setupDB()
      val entries = Random.shuffle(createIndexedType())
      entries.foreach(db.write)

      val view = db.view(classOf[IndexedType])

      benchmark.addTimerCase("Ascending") { timer =>
        timer.startTiming()
        val it = view.index("name").closeableIterator()
        timer.stopTiming()
        it.close()
      }

      benchmark.addTimerCase("Descending") { timer =>
        timer.startTiming()
        val it = view.index("name").reverse().closeableIterator()
        timer.stopTiming()
        it.close()
      }

      benchmark.run()
    } finally {
      cleanupDB()
    }
  }

  private def refIndexIteration(): Unit = {
    val benchmark = new Benchmark("Ref Index - Iteration", COUNT, ITERATIONS, output = output)

    try {
      setupDB()
      val entries = Random.shuffle(createIndexedType())
      entries.foreach(db.write)

      val view = db.view(classOf[IndexedType])

      benchmark.addTimerCase("Ascending") { timer =>
        val it = view.index("name").closeableIterator()
        try {
          timer.startTiming()
          while (it.hasNext) {
            it.next()
          }
          timer.stopTiming()
        } finally {
          it.close()
        }
      }

      benchmark.addTimerCase("Descending") { timer =>
        val it = view.index("name").reverse().closeableIterator()
        try {
          timer.startTiming()
          while (it.hasNext) {
            it.next()
          }
          timer.stopTiming()
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

  private def noIndexWrite(entries: Seq[SimpleType], timer: Benchmark.Timer): Unit = {
    entries.foreach { e =>
      timer.startTiming()
      db.write(e)
      timer.stopTiming()
    }
  }

  private def noIndexUpdate(entries: Seq[SimpleType], timer: Benchmark.Timer): Unit = {
    entries.foreach { e =>
      e.name = s"updated_${e.name}"
      timer.startTiming()
      db.write(e)
      timer.stopTiming()
    }
  }

  private def noIndexDelete(entries: Seq[SimpleType], timer: Benchmark.Timer): Unit = {
    entries.foreach { e =>
      timer.startTiming()
      db.delete(e.getClass, e.key)
      timer.stopTiming()
    }
  }

  private def indexWrite(entries: Seq[IndexedType], timer: Benchmark.Timer): Unit = {
    entries.foreach { e =>
      timer.startTiming()
      db.write(e)
      timer.stopTiming()
    }
  }

  private def indexUpdate(entries: Seq[IndexedType], timer: Benchmark.Timer): Unit = {
    entries.foreach { e =>
      e.name = s"updated_${e.name}"
      timer.startTiming()
      db.write(e)
      timer.stopTiming()
    }
  }

  private def indexDelete(entries: Seq[IndexedType], timer: Benchmark.Timer): Unit = {
    entries.foreach { e =>
      timer.startTiming()
      db.delete(e.getClass, e.key)
      timer.stopTiming()
    }
  }
}

private class SimpleType {
  @KVIndex var key = 0
  var name: String = _
}

private class IndexedType {
  @KVIndex var key = 0
  @KVIndex("name") @BeanProperty var name: String = _
}
