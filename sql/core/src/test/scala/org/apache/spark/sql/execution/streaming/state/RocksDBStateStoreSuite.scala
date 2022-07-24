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
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

class RocksDBStateStoreSuite extends StateStoreSuiteBase[RocksDBStateStoreProvider]
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

  test("version encoding") {
    import RocksDBStateStoreProvider._

    tryWithProviderResource(newStoreProvider()) { provider =>
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
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      // Set the session confs that should be passed into RocksDB
      val testConfs = Seq(
        ("spark.sql.streaming.stateStore.providerClass",
          classOf[RocksDBStateStoreProvider].getName),
        (RocksDBConf.ROCKSDB_CONF_NAME_PREFIX + ".compactOnCommit", "true"),
        (RocksDBConf.ROCKSDB_CONF_NAME_PREFIX + ".lockAcquireTimeoutMs", "10"),
        (RocksDBConf.ROCKSDB_CONF_NAME_PREFIX + ".maxOpenFiles", "1000"),
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
      assert(rocksDBConfInTask.lockAcquireTimeoutMs == 10L)
      assert(rocksDBConfInTask.formatVersion == 4)
      assert(rocksDBConfInTask.maxOpenFiles == 1000)
    }
  }

  test("rocksdb file manager metrics exposed") {
    import RocksDBStateStoreProvider._
    def getCustomMetric(metrics: StateStoreMetrics, customMetric: StateStoreCustomMetric): Long = {
      val metricPair = metrics.customMetrics.find(_._1.name == customMetric.name)
      assert(metricPair.isDefined)
      metricPair.get._2
    }

    tryWithProviderResource(newStoreProvider()) { provider =>
      val store = provider.getStore(0)
      // Verify state after updating
      put(store, "a", 0, 1)
      assert(get(store, "a", 0) === Some(1))
      assert(store.commit() === 1)
      assert(store.hasCommitted)
      val storeMetrics = store.metrics
      assert(storeMetrics.numKeys === 1)
      assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_FILES_COPIED) > 0L)
      assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_FILES_REUSED) == 0L)
      assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_BYTES_COPIED) > 0L)
      assert(getCustomMetric(storeMetrics, CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED) > 0L)
    }
  }

  override def newStoreProvider(): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0))
  }

  def newStoreProvider(storeId: StateStoreId): RocksDBStateStoreProvider = {
    newStoreProvider(storeId, numColsPrefixKey = 0)
  }

  override def newStoreProvider(numPrefixCols: Int): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0), numColsPrefixKey = numPrefixCols)
  }

  def newStoreProvider(
      storeId: StateStoreId,
      numColsPrefixKey: Int): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId, keySchema, valueSchema, numColsPrefixKey = numColsPrefixKey,
      new StateStoreConf, new Configuration)
    provider
  }

  override def getLatestData(
      storeProvider: RocksDBStateStoreProvider): Set[((String, Int), Int)] = {
    getData(storeProvider, version = -1)
  }

  override def getData(
      provider: RocksDBStateStoreProvider,
      version: Int = -1): Set[((String, Int), Int)] = {
    tryWithProviderResource(newStoreProvider(provider.stateStoreId)) { reloadedProvider =>
      val versionToRead = if (version < 0) reloadedProvider.latestVersion else version
      reloadedProvider.getStore(versionToRead).iterator().map(rowPairToDataPair).toSet
    }
  }

  override def newStoreProvider(
    minDeltasForSnapshot: Int,
    numOfVersToRetainInMemory: Int): RocksDBStateStoreProvider = newStoreProvider()

  override def getDefaultSQLConf(
    minDeltasForSnapshot: Int,
    numOfVersToRetainInMemory: Int): SQLConf = new SQLConf()
}

