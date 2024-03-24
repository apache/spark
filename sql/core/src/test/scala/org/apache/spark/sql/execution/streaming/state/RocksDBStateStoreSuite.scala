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

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

@ExtendedSQLTest
class RocksDBStateStoreSuite extends StateStoreSuiteBase[RocksDBStateStoreProvider]
  with AlsoTestWithChangelogCheckpointingEnabled
  with SharedSparkSession
  with BeforeAndAfter {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  import StateStoreTestsHelper._

  testWithColumnFamilies(s"version encoding",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    import RocksDBStateStoreProvider._

    tryWithProviderResource(newStoreProvider(colFamiliesEnabled)) { provider =>
      val store = provider.getStore(0)
      val keyRow = dataToKeyRow("a", 0)
      val valueRow = dataToValueRow(1)
      store.put(keyRow, valueRow)
      val iter = provider.rocksDB.iterator()
      assert(iter.hasNext)
      val kv = iter.next()

      // Verify the version encoded in first byte of the key and value byte arrays
      assert(Platform.getByte(kv.key, Platform.BYTE_ARRAY_OFFSET) === STATE_ENCODING_VERSION)
      assert(Platform.getByte(kv.value, Platform.BYTE_ARRAY_OFFSET) === STATE_ENCODING_VERSION)
    }
  }

  test("RocksDB confs are passed correctly from SparkSession to db instance") {
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    withSparkSession(SparkSession.builder().config(sparkConf).getOrCreate()) { spark =>
      // Set the session confs that should be passed into RocksDB
      val testConfs = Seq(
        ("spark.sql.streaming.stateStore.providerClass",
          classOf[RocksDBStateStoreProvider].getName),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".compactOnCommit", "true"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled", "true"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".lockAcquireTimeoutMs", "10"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".maxOpenFiles", "1000"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".maxWriteBufferNumber", "3"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".writeBufferSizeMB", "16"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".allowFAllocate", "false"),
        (RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".compression", "zstd"),
        (SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION.key, "4")
      )
      testConfs.foreach { case (k, v) => spark.conf.set(k, v) }

      // Prepare test objects for running task on state store
      val testRDD = spark.sparkContext.makeRDD[String](Seq("a"), 1)
      val testSchema = StructType(Seq(StructField("key", StringType, true)))
      val testStateInfo = StatefulOperatorStateInfo(
        checkpointLocation = Utils.createTempDir().getAbsolutePath,
        queryRunId = UUID.randomUUID, operatorId = 0, storeVersion = 0, numPartitions = 5)

      // Create state store in a task and get the RocksDBConf from the instantiated RocksDB instance
      val rocksDBConfInTask: RocksDBConf = testRDD.mapPartitionsWithStateStore[RocksDBConf](
        spark.sqlContext, testStateInfo, testSchema, testSchema, 0) {
          (store: StateStore, _: Iterator[String]) =>
            // Use reflection to get RocksDB instance
            val dbInstanceMethod =
              store.getClass.getMethods.filter(_.getName.contains("dbInstance")).head
            Iterator(dbInstanceMethod.invoke(store).asInstanceOf[RocksDB].conf)
        }.collect().head

      // Verify the confs are same as those configured in the session conf
      assert(rocksDBConfInTask.compactOnCommit == true)
      assert(rocksDBConfInTask.enableChangelogCheckpointing == true)
      assert(rocksDBConfInTask.lockAcquireTimeoutMs == 10L)
      assert(rocksDBConfInTask.formatVersion == 4)
      assert(rocksDBConfInTask.maxOpenFiles == 1000)
      assert(rocksDBConfInTask.maxWriteBufferNumber == 3)
      assert(rocksDBConfInTask.writeBufferSizeMB == 16L)
      assert(rocksDBConfInTask.allowFAllocate == false)
      assert(rocksDBConfInTask.compression == "zstd")
    }
  }

  testWithColumnFamilies("rocksdb file manager metrics exposed",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    import RocksDBStateStoreProvider._
    def getCustomMetric(metrics: StateStoreMetrics,
      customMetric: StateStoreCustomMetric): Long = {
      val metricPair = metrics.customMetrics.find(_._1.name == customMetric.name)
      assert(metricPair.isDefined)
      metricPair.get._2
    }

    withSQLConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1") {
      tryWithProviderResource(newStoreProvider(colFamiliesEnabled)) { provider =>
        val store = provider.getStore(0)
        // Verify state after updating
        put(store, "a", 0, 1)
        assert(get(store, "a", 0) === Some(1))
        assert(store.commit() === 1)
        provider.doMaintenance()
        assert(store.hasCommitted)
        val storeMetrics = store.metrics
        assert(storeMetrics.numKeys === 1)
        // SPARK-46249 - In the case of changelog checkpointing, the snapshot upload happens in
        // the context of the background maintenance thread. The file manager metrics are updated
        // here and will be available as part of the next metrics update. So we cannot rely on
        // the file manager metrics to be available here for this version.
        if (!isChangelogCheckpointingEnabled) {
          assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_FILES_COPIED) > 0L)
          assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_FILES_REUSED) == 0L)
          assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_BYTES_COPIED) > 0L)
          assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED) > 0L)
        }
      }
    }
  }

  testWithColumnFamilies("rocksdb key and value schema encoders for column families",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    val testColFamily = "testState"

    tryWithProviderResource(newStoreProvider(colFamiliesEnabled)) { provider =>
      val store = provider.getStore(0)
      if (colFamiliesEnabled) {
        store.createColFamilyIfAbsent(testColFamily, keySchema, numColsPrefixKey = 0, valueSchema)
        val keyRow1 = dataToKeyRow("a", 0)
        val valueRow1 = dataToValueRow(1)
        store.put(keyRow1, valueRow1, colFamilyName = testColFamily)
        assert(valueRowToData(store.get(keyRow1, colFamilyName = testColFamily)) === 1)
        store.remove(keyRow1, colFamilyName = testColFamily)
        assert(store.get(keyRow1, colFamilyName = testColFamily) === null)
      }
      val keyRow2 = dataToKeyRow("b", 0)
      val valueRow2 = dataToValueRow(2)
      store.put(keyRow2, valueRow2)
      assert(valueRowToData(store.get(keyRow2)) === 2)
      store.remove(keyRow2)
      assert(store.get(keyRow2) === null)
    }
  }

  test("validate rocksdb values iterator correctness") {
    withSQLConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1") {
      tryWithProviderResource(newStoreProvider(useColumnFamilies = true,
        useMultipleValuesPerKey = true)) { provider =>
        val store = provider.getStore(0)
        // Verify state after updating
        put(store, "a", 0, 1)

        val iterator0 = store.valuesIterator(dataToKeyRow("a", 0))

        assert(iterator0.hasNext)
        assert(valueRowToData(iterator0.next()) === 1)
        assert(!iterator0.hasNext)

        merge(store, "a", 0, 2)
        merge(store, "a", 0, 3)

        val iterator1 = store.valuesIterator(dataToKeyRow("a", 0))

        (1 to 3).map { i =>
          assert(iterator1.hasNext)
          assert(valueRowToData(iterator1.next()) === i)
        }

        assert(!iterator1.hasNext)

        remove(store, _._1 == "a")
        val iterator2 = store.valuesIterator(dataToKeyRow("a", 0))
        assert(!iterator2.hasNext)

        assert(get(store, "a", 0).isEmpty)
      }
    }
  }

  override def newStoreProvider(): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0))
  }

  def newStoreProvider(storeId: StateStoreId): RocksDBStateStoreProvider = {
    newStoreProvider(storeId, numColsPrefixKey = 0)
  }

  override def newStoreProvider(storeId: StateStoreId, useColumnFamilies: Boolean):
    RocksDBStateStoreProvider = {
    newStoreProvider(storeId, numColsPrefixKey = 0, useColumnFamilies = useColumnFamilies)
  }

  override def newStoreProvider(useColumnFamilies: Boolean): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0), numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies)
  }

  def newStoreProvider(useColumnFamilies: Boolean,
      useMultipleValuesPerKey: Boolean): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0), numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies,
      useMultipleValuesPerKey = useMultipleValuesPerKey
    )
  }

  def newStoreProvider(storeId: StateStoreId, conf: Configuration): RocksDBStateStoreProvider = {
    newStoreProvider(storeId, numColsPrefixKey = -1, conf = conf)
  }

  override def newStoreProvider(numPrefixCols: Int): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0), numColsPrefixKey = numPrefixCols)
  }

  def newStoreProvider(
      storeId: StateStoreId,
      numColsPrefixKey: Int,
      sqlConf: Option[SQLConf] = None,
      conf: Configuration = new Configuration,
      useColumnFamilies: Boolean = false,
      useMultipleValuesPerKey: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId,
      keySchema,
      valueSchema,
      numColsPrefixKey = numColsPrefixKey,
      useColumnFamilies,
      new StateStoreConf(sqlConf.getOrElse(SQLConf.get)),
      conf,
      useMultipleValuesPerKey
    )
    provider
  }

  override def getLatestData(
      storeProvider: RocksDBStateStoreProvider,
      useColumnFamilies: Boolean = false): Set[((String, Int), Int)] = {
    getData(storeProvider, version = -1, useColumnFamilies)
  }

  override def getData(
      provider: RocksDBStateStoreProvider,
      version: Int = -1,
      useColumnFamilies: Boolean = false): Set[((String, Int), Int)] = {
    tryWithProviderResource(newStoreProvider(provider.stateStoreId,
      useColumnFamilies)) { reloadedProvider =>
      val versionToRead = if (version < 0) reloadedProvider.latestVersion else version
      reloadedProvider.getStore(versionToRead).iterator().map(rowPairToDataPair).toSet
    }
  }

  override def newStoreProvider(
    minDeltasForSnapshot: Int,
    numOfVersToRetainInMemory: Int): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0), 0,
      Some(getDefaultSQLConf(minDeltasForSnapshot, numOfVersToRetainInMemory)))
  }

  override def getDefaultSQLConf(
    minDeltasForSnapshot: Int,
    numOfVersToRetainInMemory: Int): SQLConf = {
    val sqlConf = SQLConf.get.clone()
    sqlConf.setConfString(
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key, minDeltasForSnapshot.toString)
    sqlConf
  }

  override def testQuietly(name: String)(f: => Unit): Unit = {
    test(name) {
      quietly {
        f
      }
    }
  }

  override def testWithAllCodec(name: String)(func: Boolean => Any): Unit = {
    Seq(true, false).foreach { colFamiliesEnabled =>
      codecsInShortName.foreach { codecName =>
        super.test(s"$name - with codec $codecName - colFamiliesEnabled=$colFamiliesEnabled") {
          withSQLConf(SQLConf.STATE_STORE_COMPRESSION_CODEC.key -> codecName) {
            func(colFamiliesEnabled)
          }
        }
      }

      CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codecName =>
        super.test(s"$name - with codec $codecName - colFamiliesEnabled=$colFamiliesEnabled") {
          withSQLConf(SQLConf.STATE_STORE_COMPRESSION_CODEC.key -> codecName) {
            func(colFamiliesEnabled)
          }
        }
      }
    }
  }
}

