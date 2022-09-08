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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.util.Utils

/**
 * Synthetic benchmark for State Store basic operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StateStoreBasicOperationsBenchmark-results.txt".
 * }}}
 */
object StateStoreBasicOperationsBenchmark extends SqlBasedBenchmark {

  private val keySchema = StructType(
    Seq(StructField("key1", IntegerType, true), StructField("key2", TimestampType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runPutBenchmark()
    runDeleteBenchmark()
    runEvictBenchmark()
  }

  final def skip(benchmarkName: String)(func: => Any): Unit = {
    output.foreach(_.write(s"$benchmarkName is skipped".getBytes))
  }

  private def runPutBenchmark(): Unit = {
    def registerPutBenchmarkCase(
        benchmark: Benchmark,
        testName: String,
        provider: StateStoreProvider,
        version: Long,
        rows: Seq[(UnsafeRow, UnsafeRow)]): Unit = {
      benchmark.addTimerCase(testName) { timer =>
        val store = provider.getStore(version)

        timer.startTiming()
        updateRows(store, rows)
        timer.stopTiming()

        store.abort()
      }
    }

    runBenchmark("put rows") {
      val numOfRows = Seq(10000)
      val overwriteRates = Seq(100, 75, 50, 25, 10, 5, 0)

      numOfRows.foreach { numOfRow =>
        val testData = constructRandomizedTestData(numOfRow,
          (1 to numOfRow).map(_ * 1000L).toList, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBWithNoTrackProvider = newRocksDBStateProvider(trackTotalNumberOfRows = false)

        val committedInMemoryVersion = loadInitialData(inMemoryProvider, testData)
        val committedRocksDBVersion = loadInitialData(rocksDBProvider, testData)
        val committedRocksDBWithNoTrackVersion = loadInitialData(
          rocksDBWithNoTrackProvider, testData)

        overwriteRates.foreach { overwriteRate =>
          val numOfRowsToOverwrite = numOfRow * overwriteRate / 100

          val numOfNewRows = numOfRow - numOfRowsToOverwrite
          val newRows = if (numOfNewRows > 0) {
            constructRandomizedTestData(numOfNewRows,
              (1 to numOfNewRows).map(_ * 1000L).toList, 0)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val existingRows = if (numOfRowsToOverwrite > 0) {
            Random.shuffle(testData).take(numOfRowsToOverwrite)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val rowsToPut = Random.shuffle(newRows ++ existingRows)

          val benchmark = new Benchmark(s"putting $numOfRow rows " +
            s"($numOfRowsToOverwrite rows to overwrite - rate $overwriteRate)",
            numOfRow, minNumIters = 10000, output = output)

          registerPutBenchmarkCase(benchmark, "In-memory", inMemoryProvider,
            committedInMemoryVersion, rowsToPut)
          registerPutBenchmarkCase(benchmark, "RocksDB (trackTotalNumberOfRows: true)",
            rocksDBProvider, committedRocksDBVersion, rowsToPut)
          registerPutBenchmarkCase(benchmark, "RocksDB (trackTotalNumberOfRows: false)",
            rocksDBWithNoTrackProvider, committedRocksDBWithNoTrackVersion, rowsToPut)

          benchmark.run()
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
        rocksDBWithNoTrackProvider.close()
      }
    }
  }

  private def runDeleteBenchmark(): Unit = {
    def registerDeleteBenchmarkCase(
        benchmark: Benchmark,
        testName: String,
        provider: StateStoreProvider,
        version: Long,
        keys: Seq[UnsafeRow]): Unit = {
      benchmark.addTimerCase(testName) { timer =>
        val store = provider.getStore(version)

        timer.startTiming()
        deleteRows(store, keys)
        timer.stopTiming()

        store.abort()
      }
    }

    runBenchmark("delete rows") {
      val numOfRows = Seq(10000)
      val nonExistRates = Seq(100, 75, 50, 25, 10, 5, 0)
      numOfRows.foreach { numOfRow =>
        val testData = constructRandomizedTestData(numOfRow,
          (1 to numOfRow).map(_ * 1000L).toList, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBWithNoTrackProvider = newRocksDBStateProvider(trackTotalNumberOfRows = false)

        val committedInMemoryVersion = loadInitialData(inMemoryProvider, testData)
        val committedRocksDBVersion = loadInitialData(rocksDBProvider, testData)
        val committedRocksDBWithNoTrackVersion = loadInitialData(
          rocksDBWithNoTrackProvider, testData)

        nonExistRates.foreach { nonExistRate =>
          val numOfRowsNonExist = numOfRow * nonExistRate / 100

          val numOfExistingRows = numOfRow - numOfRowsNonExist
          val nonExistingRows = if (numOfRowsNonExist > 0) {
            constructRandomizedTestData(numOfRowsNonExist,
              (numOfRow + 1 to numOfRow + numOfRowsNonExist).map(_ * 1000L).toList, 0)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val existingRows = if (numOfExistingRows > 0) {
            Random.shuffle(testData).take(numOfExistingRows)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val keysToDelete = Random.shuffle(nonExistingRows ++ existingRows).map(_._1)

          val benchmark = new Benchmark(s"trying to delete $numOfRow rows " +
            s"from $numOfRow rows" +
            s"($numOfRowsNonExist rows are non-existing - rate $nonExistRate)",
            numOfRow, minNumIters = 10000, output = output)

          registerDeleteBenchmarkCase(benchmark, "In-memory", inMemoryProvider,
            committedInMemoryVersion, keysToDelete)
          registerDeleteBenchmarkCase(benchmark, "RocksDB (trackTotalNumberOfRows: true)",
            rocksDBProvider, committedRocksDBVersion, keysToDelete)
          registerDeleteBenchmarkCase(benchmark, "RocksDB (trackTotalNumberOfRows: false)",
            rocksDBWithNoTrackProvider, committedRocksDBWithNoTrackVersion, keysToDelete)

          benchmark.run()
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
        rocksDBWithNoTrackProvider.close()
      }
    }
  }

  private def runEvictBenchmark(): Unit = {
    def registerEvictBenchmarkCase(
        benchmark: Benchmark,
        testName: String,
        provider: StateStoreProvider,
        version: Long,
        maxTimestampToEvictInMillis: Long,
        expectedNumOfRows: Long): Unit = {
      benchmark.addTimerCase(testName) { timer =>
        val store = provider.getStore(version)

        timer.startTiming()
        evictAsFullScanAndRemove(store, maxTimestampToEvictInMillis,
          expectedNumOfRows)
        timer.stopTiming()

        store.abort()
      }
    }

    runBenchmark("evict rows") {
      val numOfRows = Seq(10000)
      val numOfEvictionRates = Seq(100, 75, 50, 25, 10, 5, 0)

      numOfRows.foreach { numOfRow =>
        val timestampsInMicros = (0L until numOfRow).map(ts => ts * 1000L).toList

        val testData = constructRandomizedTestData(numOfRow, timestampsInMicros, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBWithNoTrackProvider = newRocksDBStateProvider(trackTotalNumberOfRows = false)

        val committedInMemoryVersion = loadInitialData(inMemoryProvider, testData)
        val committedRocksDBVersion = loadInitialData(rocksDBProvider, testData)
        val committedRocksDBWithNoTrackVersion = loadInitialData(
          rocksDBWithNoTrackProvider, testData)

        numOfEvictionRates.foreach { numOfEvictionRate =>
          val numOfRowsToEvict = numOfRow * numOfEvictionRate / 100
          val maxTimestampToEvictInMillis = timestampsInMicros
            .take(numOfRow * numOfEvictionRate / 100)
            .lastOption.map(_ / 1000).getOrElse(-1L)

          val benchmark = new Benchmark(s"evicting $numOfRowsToEvict rows " +
            s"(maxTimestampToEvictInMillis: $maxTimestampToEvictInMillis) " +
            s"from $numOfRow rows",
            numOfRow, minNumIters = 10000, output = output)

          registerEvictBenchmarkCase(benchmark, "In-memory", inMemoryProvider,
            committedInMemoryVersion, maxTimestampToEvictInMillis, numOfRowsToEvict)

          registerEvictBenchmarkCase(benchmark, "RocksDB (trackTotalNumberOfRows: true)",
            rocksDBProvider, committedRocksDBVersion, maxTimestampToEvictInMillis,
            numOfRowsToEvict)

          registerEvictBenchmarkCase(benchmark, "RocksDB (trackTotalNumberOfRows: false)",
            rocksDBWithNoTrackProvider, committedRocksDBWithNoTrackVersion,
            maxTimestampToEvictInMillis, numOfRowsToEvict)

          benchmark.run()
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
        rocksDBWithNoTrackProvider.close()
      }
    }
  }

  private def getRows(store: StateStore, keys: Seq[UnsafeRow]): Seq[UnsafeRow] = {
    keys.map(store.get)
  }

  private def loadInitialData(
      provider: StateStoreProvider,
      data: Seq[(UnsafeRow, UnsafeRow)]): Long = {
    val store = provider.getStore(0)
    updateRows(store, data)
    store.commit()
  }

  private def updateRows(
      store: StateStore,
      rows: Seq[(UnsafeRow, UnsafeRow)]): Unit = {
    rows.foreach { case (key, value) =>
      store.put(key, value)
    }
  }

  private def deleteRows(
      store: StateStore,
      rows: Seq[UnsafeRow]): Unit = {
    rows.foreach { key =>
      store.remove(key)
    }
  }

  private def evictAsFullScanAndRemove(
      store: StateStore,
      maxTimestampToEvictMillis: Long,
      expectedNumOfRows: Long): Unit = {
    var removedRows: Long = 0
    store.iterator().foreach { r =>
      if (r.key.getLong(1) <= maxTimestampToEvictMillis * 1000L) {
        store.remove(r.key)
        removedRows += 1
      }
    }
    assert(removedRows == expectedNumOfRows,
      s"expected: $expectedNumOfRows actual: $removedRows")
  }

  // This prevents created keys to be in order, which may affect the performance on RocksDB.
  private def constructRandomizedTestData(
      numRows: Int,
      timestamps: List[Long],
      minIdx: Int = 0): Seq[(UnsafeRow, UnsafeRow)] = {
    assert(numRows >= timestamps.length)
    assert(numRows % timestamps.length == 0)

    (1 to numRows).map { idx =>
      val keyRow = new GenericInternalRow(2)
      keyRow.setInt(0, Random.nextInt(Int.MaxValue))
      keyRow.setLong(1, timestamps((minIdx + idx) % timestamps.length)) // microseconds
      val valueRow = new GenericInternalRow(1)
      valueRow.setInt(0, minIdx + idx)

      val keyUnsafeRow = keyProjection(keyRow).copy()
      val valueUnsafeRow = valueProjection(valueRow).copy()

      (keyUnsafeRow, valueUnsafeRow)
    }
  }

  private def newHDFSBackedStateStoreProvider(): StateStoreProvider = {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new HDFSBackedStateStoreProvider()
    val storeConf = new StateStoreConf(new SQLConf())
    provider.init(
      storeId, keySchema, valueSchema, 0,
      storeConf, new Configuration)
    provider
  }

  private def newRocksDBStateProvider(
      trackTotalNumberOfRows: Boolean = true): StateStoreProvider = {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new RocksDBStateStoreProvider()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows",
      trackTotalNumberOfRows.toString)
    val storeConf = new StateStoreConf(sqlConf)

    provider.init(
      storeId, keySchema, valueSchema, 0,
      storeConf, new Configuration)
    provider
  }

  private def newDir(): String = Utils.createTempDir().toString
}
