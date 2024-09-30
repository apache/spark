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

import org.apache.hadoop.conf.Configuration
import org.scalatest.Tag

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.types.StructType

object CkptIdCollectingStateStoreWrapper {
  // Internal list to hold checkpoint IDs (strings)
  private var checkpointInfos: List[StateStoreCheckpointInfo] = List.empty

  // Method to add a string (checkpoint ID) to the list in a synchronized way
  def addCheckpointInfo(checkpointID: StateStoreCheckpointInfo): Unit = synchronized {
    checkpointInfos = checkpointID :: checkpointInfos
  }

  // Method to read the list of checkpoint IDs in a synchronized way
  def getStateStoreCheckpointInfos: List[StateStoreCheckpointInfo] = synchronized {
    checkpointInfos
  }

  def clear(): Unit = synchronized {
    checkpointInfos = List.empty
  }
}

case class CkptIdCollectingStateStoreWrapper(innerStore: StateStore) extends StateStore {

  // Implement methods from ReadStateStore (parent trait)

  override def id: StateStoreId = innerStore.id
  override def version: Long = innerStore.version

  override def get(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): UnsafeRow = {
    innerStore.get(key, colFamilyName)
  }

  override def valuesIterator(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRow] = {
    innerStore.valuesIterator(key, colFamilyName)
  }

  override def prefixScan(
      prefixKey: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair] = {
    innerStore.prefixScan(prefixKey, colFamilyName)
  }

  override def iterator(
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair] = {
    innerStore.iterator(colFamilyName)
  }

  override def abort(): Unit = innerStore.abort()

  // Implement methods from StateStore (current trait)

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    innerStore.removeColFamilyIfExists(colFamilyName)
  }

  override def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean = false,
      isInternal: Boolean = false): Unit = {
    innerStore.createColFamilyIfAbsent(
      colFamilyName,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useMultipleValuesPerKey,
      isInternal
    )
  }

  override def put(
      key: UnsafeRow,
      value: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    innerStore.put(key, value, colFamilyName)
  }

  override def remove(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    innerStore.remove(key, colFamilyName)
  }

  override def merge(
      key: UnsafeRow,
      value: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    innerStore.merge(key, value, colFamilyName)
  }

  override def commit(): Long = innerStore.commit()
  override def metrics: StateStoreMetrics = innerStore.metrics
  override def getStateStoreCheckpointInfo: StateStoreCheckpointInfo = {
    val ret = innerStore.getStateStoreCheckpointInfo
    CkptIdCollectingStateStoreWrapper.addCheckpointInfo(ret)
    ret
  }
  override def hasCommitted: Boolean = innerStore.hasCommitted
}

// Wrapper class implementing StateStoreProvider
class CkptIdCollectingStateStoreProviderWrapper extends StateStoreProvider {

  val innerProvider = new RocksDBStateStoreProvider()

  // Now, delegate all methods in the wrapper class to the inner object
  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): Unit = {
    innerProvider.init(
      stateStoreId,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies,
      storeConfs,
      hadoopConf,
      useMultipleValuesPerKey
    )
  }

  override def stateStoreId: StateStoreId = innerProvider.stateStoreId

  override def close(): Unit = innerProvider.close()

  override def getStore(version: Long, stateStoreCkptId: Option[String] = None): StateStore = {
    val innerStateStore = innerProvider.getStore(version, stateStoreCkptId)
    CkptIdCollectingStateStoreWrapper(innerStateStore)
  }

  override def getReadStore(version: Long, uniqueId: Option[String] = None): ReadStateStore = {
    new WrappedReadStateStore(
      CkptIdCollectingStateStoreWrapper(innerProvider.getReadStore(version, uniqueId)))
  }

  override def doMaintenance(): Unit = innerProvider.doMaintenance()

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] =
    innerProvider.supportedCustomMetrics
}

class RocksDBStateStoreCheckpointFormatV2Suite extends StreamTest
  with AlsoTestWithChangelogCheckpointingEnabled {
  import testImplicits._

  val providerClassName = classOf[CkptIdCollectingStateStoreProviderWrapper].getCanonicalName


  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    CkptIdCollectingStateStoreWrapper.clear()
  }

  def testWithCheckpointInfoTracked(testName: String, testTags: Tag*)(
      testBody: => Any): Unit = {
    super.testWithChangelogCheckpointingEnabled(testName, testTags: _*) {
      super.beforeEach()
      withSQLConf(
        (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName),
        (SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2"),
        (SQLConf.SHUFFLE_PARTITIONS.key, "2")) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }
  }

  // This test enable checkpoint format V2 without validating the checkpoint ID. Just to make
  // sure it doesn't break and return the correct query results.
  testWithChangelogCheckpointingEnabled(s"checkpointFormatVersion2") {
    withSQLConf((SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "2")) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[Int]
        val aggregated =
          inputData
            .toDF()
            .groupBy($"value")
            .agg(count("*"))
            .as[(Int, Long)]

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 3),
          CheckLastBatch((3, 1)),
          AddData(inputData, 3, 2),
          CheckLastBatch((3, 2), (2, 1)),
          StopStream
        )

        // Run the stream with changelog checkpointing enabled.
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 3, 2, 1),
          CheckLastBatch((3, 3), (2, 2), (1, 1)),
          // By default we run in new tuple mode.
          AddData(inputData, 4, 4, 4, 4),
          CheckLastBatch((4, 4)),
          AddData(inputData, 5, 5),
          CheckLastBatch((5, 2))
        )

        // Run the stream with changelog checkpointing disabled.
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 4),
          CheckLastBatch((4, 5))
        )
      }
    }
  }

  def validateBaseCheckpointInfo(): Unit = {
    val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos
    // Here we assume for every task, we fetch checkpointID from the N state stores in the same
    // order. So we can separate stateStoreCkptId for different stores based on the order inside the
    // same (batchId, partitionId) group.
    val grouped = checkpointInfoList
      .groupBy(info => (info.batchVersion, info.partitionId))
      .values
      .flatMap { infos =>
        infos.zipWithIndex.map { case (info, index) => index -> info }
      }
      .groupBy(_._1)
      .map {
        case (_, grouped) =>
          grouped.map { case (_, info) => info }
      }

    grouped.foreach { l =>
      for {
        a <- l
        b <- l
        if a.partitionId == b.partitionId && a.batchVersion == b.batchVersion + 1
      } {
        // if batch version exists, it should be the same as the checkpoint ID of the previous batch
        assert(!a.baseStateStoreCkptId.isDefined || b.stateStoreCkptId == a.baseStateStoreCkptId)
      }
    }
  }

  def validateCheckpointInfo(
      numBatches: Int,
      numStateStores: Int,
      batchVersionSet: Set[Long]): Unit = {
    val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos
    // We have 6 batches, 2 partitions, and 1 state store per batch
    assert(checkpointInfoList.size == numBatches * numStateStores * 2)
    checkpointInfoList.foreach { l =>
      assert(l.stateStoreCkptId.isDefined)
      if (batchVersionSet.contains(l.batchVersion)) {
        assert(l.baseStateStoreCkptId.isDefined)
      }
    }
    assert(checkpointInfoList.count(_.partitionId == 0) == numBatches * numStateStores)
    assert(checkpointInfoList.count(_.partitionId == 1) == numBatches * numStateStores)
    for (i <- 1 to numBatches) {
      assert(checkpointInfoList.count(_.batchVersion == i) == numStateStores * 2)
    }
    validateBaseCheckpointInfo()
  }

  testWithCheckpointInfoTracked(s"checkpointFormatVersion2 validate ID") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3),
        CheckLastBatch((3, 1)),
        AddData(inputData, 3, 2),
        CheckLastBatch((3, 2), (2, 1)),
        StopStream
      )

      // Test recovery
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3, 2, 1),
        CheckLastBatch((3, 3), (2, 2), (1, 1)),
        // By default we run in new tuple mode.
        AddData(inputData, 4, 4, 4, 4),
        CheckLastBatch((4, 4)),
        AddData(inputData, 5, 5),
        CheckLastBatch((5, 2)),
        StopStream
      )

      // crash recovery again
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4),
        CheckLastBatch((4, 5))
      )
    }

    validateCheckpointInfo(6, 1, Set(2, 4, 5))
  }

  testWithCheckpointInfoTracked(
    s"checkpointFormatVersion2 validate ID with dedup and groupBy") {
    withTempDir { checkpointDir =>

      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .dropDuplicates("value") // Deduplication operation
          .groupBy($"value") // Group-by operation
          .agg(count("*"))
          .as[(Int, Long)]

      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3),
        CheckLastBatch((3, 1)),
        AddData(inputData, 3, 2),
        CheckLastBatch((2, 1)), // 3 is deduplicated
        StopStream
      )
      // Test recovery
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3, 2, 1),
        CheckLastBatch((1, 1)), // 2,3 is deduplicated
        AddData(inputData, 4, 4, 4, 4),
        CheckLastBatch((4, 1)),
        AddData(inputData, 5, 5),
        CheckLastBatch((5, 1)),
        StopStream
      )
      // Crash recovery again
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4),
        CheckLastBatch(), // 4 is deduplicated
        StopStream
      )
    }
    validateCheckpointInfo(6, 2, Set(2, 4, 5))
  }

  testWithCheckpointInfoTracked(
    s"checkpointFormatVersion2 validate ID for stream-stream join") {
    withTempDir { checkpointDir =>
      val inputData1 = MemoryStream[Int]
      val inputData2 = MemoryStream[Int]

      val df1 = inputData1.toDS().toDF("value")
      val df2 = inputData2.toDS().toDF("value")

      val joined = df1.join(df2, df1("value") === df2("value"))

      testStream(joined, OutputMode.Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData1, 3, 2),
        AddData(inputData2, 3),
        CheckLastBatch((3, 3)),
        AddData(inputData2, 2),
        // This data will be used after restarting the query
        AddData(inputData1, 5),
        CheckLastBatch((2, 2)),
        StopStream
      )

      // Test recovery.
      testStream(joined, OutputMode.Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData1, 4),
        AddData(inputData2, 5),
        CheckLastBatch((5, 5)),
        AddData(inputData2, 4),
        // This data will be used after restarting the query
        AddData(inputData1, 7),
        CheckLastBatch((4, 4)),
        StopStream
      )

      // recovery again
      testStream(joined, OutputMode.Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData1, 6),
        AddData(inputData2, 6),
        CheckLastBatch((6, 6)),
        AddData(inputData2, 7),
        CheckLastBatch((7, 7)),
        StopStream
      )
    }
    val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos
    // We sometimes add data to both data sources before CheckLastBatch(). They could be picked
    // up by one or two batches. There will be at least 6 batches, but less than 12.
    assert(checkpointInfoList.size % 8 == 0)
    val numBatches = checkpointInfoList.size / 8

    // We don't pass batch versions that would need base checkpoint IDs because we don't know
    // batchIDs for that. We only know that there are 3 batches without it.
    validateCheckpointInfo(numBatches, 4, Set())
    assert(CkptIdCollectingStateStoreWrapper
      .getStateStoreCheckpointInfos
      .count(_.baseStateStoreCkptId.isDefined) == (numBatches - 3) * 8)
  }

  testWithCheckpointInfoTracked(s"checkpointFormatVersion2 validate DropDuplicates") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val deduplicated = inputData
        .toDF()
        .dropDuplicates("value")
        .as[Int]

      testStream(deduplicated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3),
        CheckLastBatch(3),
        AddData(inputData, 3, 2),
        CheckLastBatch(2),
        AddData(inputData, 3, 2, 1),
        CheckLastBatch(1),
        StopStream
      )

      // Test recovery
      testStream(deduplicated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4, 1, 3),
        CheckLastBatch(4),
        AddData(inputData, 5, 4, 4),
        CheckLastBatch(5),
        StopStream
      )

      // crash recovery again
      testStream(deduplicated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4, 7),
        CheckLastBatch(7)
      )
    }
    validateCheckpointInfo(6, 1, Set(2, 3, 5))
  }

  testWithCheckpointInfoTracked(
    s"checkpointFormatVersion2 validate FlatMapGroupsWithState") {
    withTempDir { checkpointDir =>
      val stateFunc = (key: Int, values: Iterator[Int], state: GroupState[Int]) => {
        val count: Int = state.getOption.getOrElse(0) + values.size
        state.update(count)
        Iterator((key, count))
      }

      val inputData = MemoryStream[Int]
      val aggregated = inputData
        .toDF()
        .toDF("key")
        .selectExpr("key")
        .as[Int]
        .repartition($"key")
        .groupByKey(x => x)
        .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.NoTimeout())(stateFunc)

      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3),
        CheckLastBatch((3, 1)),
        AddData(inputData, 3, 2),
        CheckLastBatch((3, 2), (2, 1)),
        StopStream
      )

      // Test recovery
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4, 1, 3),
        CheckLastBatch((4, 1), (1, 1), (3, 3)),
        AddData(inputData, 5, 4, 4),
        CheckLastBatch((5, 1), (4, 3)),
        StopStream
      )

      // crash recovery again
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4, 7),
        CheckLastBatch((4, 4), (7, 1)),
        AddData (inputData, 5),
        CheckLastBatch((5, 2)),
        StopStream
      )
    }
    validateCheckpointInfo(6, 1, Set(2, 4, 6))
  }
}
