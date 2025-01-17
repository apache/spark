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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.Tag

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.{DataFrame, ForeachWriter}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.{CommitLog, MemoryStream}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.test.TestSparkSession
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
  override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
    val ret = innerStore.getStateStoreCheckpointInfo()
    CkptIdCollectingStateStoreWrapper.addCheckpointInfo(ret)
    ret
  }
  override def hasCommitted: Boolean = innerStore.hasCommitted
}

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
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit = {
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
  with AlsoTestWithRocksDBFeatures {
  import testImplicits._

  val providerClassName = classOf[CkptIdCollectingStateStoreProviderWrapper].getCanonicalName

  // Force test task retry number to be 2
  protected override def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[1, 2]", this.getClass.getSimpleName, sparkConf))
  }

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
        (SQLConf.SHUFFLE_PARTITIONS.key, "2"),
        // Set the minDeltasForSnapshot to 2 to force a snapshot after every 2 microbatches
        // This is to make sure the lineage is also verified for snapshots
        (SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key, "2")
      ) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }
  }

  val changelogEnabled =
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled" -> "true"
  val changelogDisabled =
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled" -> "false"
  val ckptv2 = SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2"

  case class checkpointV2CompatibilityTest(
    configsForFirstRun: Seq[(String, String)],
    configsForSecondRun: Seq[(String, String)]
  ) {
    override def toString: String = "first run - " +
      s"changeLogEnabled: ${configsForFirstRun(0)._2}, ckpt ver: ${configsForFirstRun(1)._2}" +
      s" - second run - changeLogEnabled: ${configsForSecondRun(0)._2}, " +
      s"ckpt ver: ${configsForSecondRun(1)._2}"
  }

  val testConfigSetups = Seq(
    // Enable and disable changelog under ckpt v2
    checkpointV2CompatibilityTest(Seq(changelogEnabled, ckptv2), Seq(changelogDisabled, ckptv2)),
    checkpointV2CompatibilityTest(Seq(changelogDisabled, ckptv2), Seq(changelogEnabled, ckptv2))
  )

  testConfigSetups.foreach { testConfig =>
    testWithRocksDBStateStore("checkpointFormatVersion2 Compatibility - simple agg - " +
      s"$testConfig") {
        withTempDir { checkpointDir =>
          val inputData = MemoryStream[Int]
          val aggregated =
            inputData
              .toDF()
              .groupBy($"value")
              .agg(count("*"))
              .as[(Int, Long)]

          withSQLConf(testConfig.configsForFirstRun: _*) {
            testStream(aggregated, Update)(
              StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
              AddData(inputData, 3),
              CheckLastBatch((3, 1)),
              AddData(inputData, 3, 2),
              CheckLastBatch((3, 2), (2, 1)),
              AddData(inputData, 10),
              CheckLastBatch((10, 1)),
              StopStream
            )

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
          }

          withSQLConf(testConfig.configsForSecondRun: _*) {
            testStream(aggregated, Update)(
              StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
              AddData(inputData, 4),
              CheckLastBatch((4, 5))
            )
          }
        }
      }
  }

  testWithRocksDBStateStore("checkpointFormatVersion2 Compatibility - simple agg - " +
    "v2 - changelog on -> off -> on") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

      withSQLConf(changelogEnabled, ckptv2) {
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 3),
          CheckLastBatch((3, 1)),
          AddData(inputData, 3, 2),
          CheckLastBatch((3, 2), (2, 1)),
          StopStream
        )

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
      }

      withSQLConf(changelogDisabled, ckptv2) {
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 4),
          CheckLastBatch((4, 5)),
          AddData(inputData, 4),
          CheckLastBatch((4, 6)),
          StopStream
        )

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 6, 7, 8),
          CheckLastBatch((6, 1), (7, 1), (8, 1)),
          AddData(inputData, 4, 4, 4, 4),
          CheckLastBatch((4, 10)),
          AddData(inputData, 5, 5),
          CheckLastBatch((5, 4)),
          StopStream
        )
      }

      withSQLConf(changelogDisabled, ckptv2) {
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 3, 2, 1),
          CheckLastBatch((3, 4), (2, 3), (1, 2)),
          AddData(inputData, 4),
          CheckLastBatch((4, 11)),
          StopStream
        )
      }
    }
  }

  testWithRocksDBStateStore("checkpointFormatVersion2 Compatibility - simple agg - " +
    "v2 - changelog off -> on -> off") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

      withSQLConf(changelogDisabled, ckptv2) {
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 3),
          CheckLastBatch((3, 1)),
          AddData(inputData, 3, 2),
          CheckLastBatch((3, 2), (2, 1)),
          StopStream
        )

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
      }

      withSQLConf(changelogEnabled, ckptv2) {
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 4),
          CheckLastBatch((4, 5)),
          AddData(inputData, 4),
          CheckLastBatch((4, 6)),
          StopStream
        )

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 6, 7, 8),
          CheckLastBatch((6, 1), (7, 1), (8, 1)),
          AddData(inputData, 4, 4, 4, 4),
          CheckLastBatch((4, 10)),
          AddData(inputData, 5, 5),
          CheckLastBatch((5, 4)),
          StopStream
        )
      }

      withSQLConf(changelogDisabled, ckptv2) {
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, 3, 2, 1),
          CheckLastBatch((3, 4), (2, 3), (1, 2)),
          AddData(inputData, 4),
          CheckLastBatch((4, 11)),
          StopStream
        )
      }
    }
  }

  testConfigSetups.foreach { testConfig =>
    testWithRocksDBStateStore("checkpointFormatVersion2 Compatibility - " +
      s"transformWithState - $testConfig") {
        withTempDir { checkpointDir =>
          val inputData = MemoryStream[String]
          val result = inputData.toDS()
            .groupByKey(x => x)
            .transformWithState(new RunningCountStatefulProcessor(),
              TimeMode.None(),
              OutputMode.Update())

          withSQLConf(testConfig.configsForFirstRun: _*) {
            testStream(result, Update())(
              StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
              AddData(inputData, "a"),
              CheckNewAnswer(("a", "1")),
              Execute { q =>
                assert(q.lastProgress.stateOperators(0)
                  .customMetrics.get("numValueStateVars") > 0)
                assert(q.lastProgress.stateOperators(0)
                  .customMetrics.get("numRegisteredTimers") == 0)
              },
              AddData(inputData, "a", "b"),
              CheckNewAnswer(("a", "2"), ("b", "1")),
              StopStream
            )
            testStream(result, Update())(
              StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
              // should remove state for "a" and not return anything for a
              AddData(inputData, "a", "b"),
              CheckNewAnswer(("b", "2")),
              StopStream
            )
          }

          withSQLConf(testConfig.configsForSecondRun: _*) {
            testStream(result, Update())(
              StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
              // should recreate state for "a" and return count as 1 and
              AddData(inputData, "a", "c"),
              CheckNewAnswer(("a", "1"), ("c", "1")),
              StopStream
            )
          }
        }
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

  /**
   * Verify lineage for each partition across batches. Below should satisfy because
   * these ids are stored in the following manner:
   * stateStoreCkptIds: id3, id2, id1
   * baseStateStoreCkptIds:  id2, id1, None
   * Below checks [id2, id1] are the same,
   * which is the lineage for this partition across batches
   */
  private def checkpointInfoLineageVerification(
      pickedCheckpointInfoList: Iterable[StateStoreCheckpointInfo]): Unit = {

    Seq(0, 1).foreach {
      partitionId =>
        val stateStoreCkptIds = pickedCheckpointInfoList
          .filter(_.partitionId == partitionId).map(_.stateStoreCkptId)
        val baseStateStoreCkptIds = pickedCheckpointInfoList
          .filter(_.partitionId == partitionId).map(_.baseStateStoreCkptId)

        assert(stateStoreCkptIds.drop(1).iterator
          .sameElements(baseStateStoreCkptIds.dropRight(1)))
    }
  }

  /**
   * Verify the version->UniqueId mapping from state store is the same as
   * that from the commit log
   * @param checkpointDir The checkpoint directory to read the commit log
   * @param pickedCheckpointInfoList The checkpoint info list to verify
   * @return true when the version->UniqueId mapping is the same as what from the commit log
   */
  private def verifyCheckpointInfoFromCommitLog(
      checkpointDir: File,
      pickedCheckpointInfoList: Iterable[StateStoreCheckpointInfo]): Boolean = {
    var ret: Boolean = true

    val versionToUniqueIdFromStateStore = Seq(1, 2).map {
      batchVersion =>
        val res = pickedCheckpointInfoList
          .filter(_.batchVersion == batchVersion).map(_.stateStoreCkptId.get)

        // batch Id is batchVersion - 1
        batchVersion - 1 -> res.toArray
    }.toMap

    val commitLogPath = new Path(
      new Path(checkpointDir.getAbsolutePath), "commits").toString

    val commitLog = new CommitLog(spark, commitLogPath)
    val metadata = commitLog.get(Some(0), Some(1)).map(_._2)

    val versionToUniqueIdFromCommitLog = metadata.zipWithIndex.map { case (metadata, idx) =>
      // Use stateUniqueIds.head(0) because there is only one state operator
      val res2 = metadata.stateUniqueIds.head(0).map { uniqueIds =>
        uniqueIds(0)
      }
      idx -> res2
    }.toMap

    versionToUniqueIdFromCommitLog.foreach {
      case (version, uniqueIds) =>
        if (!versionToUniqueIdFromStateStore(version).sorted.sameElements(uniqueIds.sorted)) {
          ret = false
          return ret
        }
    }
    ret
  }

  // scalastyle:off line.size.limit
  /**
   * When there is task retries or multiple jobs launched, there will be different
   * sets of state store checkpointInfo because of lineage. e.g.
   * [Picked] StateStoreCheckpointInfo[partitionId=1, batchVersion=2, stateStoreCkptId=Some(a9d5afec-0e8d-4473-b948-6c55513aa509), baseStateStoreCkptId=Some(061f7c53-b300-477a-a599-5387d55e315a)]
   * [Picked] StateStoreCheckpointInfo[partitionId=0, batchVersion=2, stateStoreCkptId=Some(879cc517-6b85-4dae-abba-794bf2dbab82), baseStateStoreCkptId=Some(513726e7-2448-41a6-a874-92053c5cf86b)]
   * StateStoreCheckpointInfo[partitionId=1, batchVersion=2, stateStoreCkptId=Some(7f4ad39f-d019-4ca2-8cf4-300379821cd6), baseStateStoreCkptId=Some(061f7c53-b300-477a-a599-5387d55e315a)]
   * StateStoreCheckpointInfo[partitionId=0, batchVersion=2, stateStoreCkptId=Some(9dc215fe-54f9-4dc1-a59b-a8734f359e46), baseStateStoreCkptId=Some(513726e7-2448-41a6-a874-92053c5cf86b)]
   * This function finds the correct one based on commit log
   */
  // scalastyle:on line.size.limit
   private def pickCheckpointInfoFromCommitLog(
        checkpointDir: File,
        checkpointInfoList: Iterable[StateStoreCheckpointInfo]
        ): Option[Iterable[StateStoreCheckpointInfo]] = {
      val allPossibleCheckpointInfoList = checkpointInfoList
        .groupBy(x => (x.partitionId, x.batchVersion)).values.transpose

     var ret: Option[Iterable[StateStoreCheckpointInfo]] = None
     for (elem <- allPossibleCheckpointInfoList) {
       if (verifyCheckpointInfoFromCommitLog(checkpointDir, elem)) {
         ret = Some(elem)
         return ret
       }
     }
     ret
   }

  /**
   * This test verifies when there are two job launched inside the
   * foreachBatch function (write to multiple sinks).
   * When this happens, the unique ids actually stored in the commit log should be the the same
   * as those recorded by CkptIdCollectingStateStoreWrapper
   */
  testWithCheckpointInfoTracked(s"checkpointFormatVersion2 validate ID - two jobs launched") {
    withTempDir { checkpointDir =>
      withTable("wei_test_t1") {
        withTable("wei_test_t2") {
          val inputData = MemoryStream[Int]
          val aggregated =
            inputData
              .toDF()
              .groupBy($"value")
              .agg(count("*"))

          val writer = (ds: DataFrame, batchId: Long) => {
            ds.write.mode("append").saveAsTable("wei_test_t1")
            ds.write.mode("append").saveAsTable("wei_test_t2")
          }

          val query = aggregated.writeStream
            .foreachBatch(writer)
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .outputMode("update")
            .start()

          inputData.addData(1 to 100)
          query.processAllAvailable()

          inputData.addData(1 to 100)
          query.processAllAvailable()

          query.stop()

          val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos

          val pickedCheckpointInfoList = pickCheckpointInfoFromCommitLog(
            checkpointDir,
            checkpointInfoList)

          assert(pickedCheckpointInfoList.isDefined,
            "No lineage information matches with commit log")

          checkpointInfoLineageVerification(pickedCheckpointInfoList.get)
        }
      }
    }
  }

  /**
   * This test verifies when there are task retries using foreachBatch.
   * When retry happens, the unique ids actually stored in the commit log should be the the same
   * as those recorded by CkptIdCollectingStateStoreWrapper
   */
  testWithCheckpointInfoTracked(s"checkpointFormatVersion2 validate ID - task retry") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))

      val writer = (ds: DataFrame, batchId: Long) => {
        val _ = ds.rdd.filter { x =>
          val context = TaskContext.get()
          // Retry in the first attempt
          if (context.attemptNumber() == 0) {
            throw new RuntimeException(s"fail the task at " +
              s"partition ${context.partitionId()} batch Id: $batchId")
          }
          x.length >= 0
        }.collect()
      }

      val query = aggregated.writeStream
        .foreachBatch(writer)
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode("update")
        .start()

      inputData.addData(1 to 100)
      query.processAllAvailable()

      inputData.addData(1 to 100)
      query.processAllAvailable()

      query.stop()

      val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos

      val pickedCheckpointInfoList = pickCheckpointInfoFromCommitLog(
        checkpointDir,
        checkpointInfoList)

      assert(pickedCheckpointInfoList.isDefined, "No lineage information matches with commit log")

      checkpointInfoLineageVerification(pickedCheckpointInfoList.get)
    }
  }

  /**
   * This test verifies when there are task retries using foreach.
   * When retry happens, the unique ids actually stored in the commit log should be the the same
   * as those recorded by CkptIdCollectingStateStoreWrapper
   */
  testWithCheckpointInfoTracked(s"checkpointFormatVersion2 validate ID - task retry - foreach") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))
          .selectExpr("CAST(value AS INT)")
          .as[Int]

      class TestForeachWriter[T] extends ForeachWriter[T] {
        def open(partitionId: Long, version: Long): Boolean = {
          true
        }

        def process(row: T): Unit = {}

        def close(errorOrNull: Throwable): Unit = {
          val context = TaskContext.get()
          // Retry in the first attempt
          if (context.attemptNumber() == 0) {
            throw new RuntimeException(s"fail the task at " +
              s"partition ${context.partitionId()}.")
          }
        }
      }

      val foreachWriter = new TestForeachWriter[Int]()

      val query = aggregated.writeStream
        .foreach(foreachWriter)
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode("update")
        .start()

      inputData.addData(1 to 100)
      query.processAllAvailable()

      inputData.addData(1 to 100)
      query.processAllAvailable()

      query.stop()

      val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos

      val pickedCheckpointInfoList = pickCheckpointInfoFromCommitLog(
        checkpointDir,
        checkpointInfoList)

      assert(pickedCheckpointInfoList.isDefined, "No lineage information matches with commit log")

      checkpointInfoLineageVerification(pickedCheckpointInfoList.get)
    }
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

      // recovery again
      testStream(joined, OutputMode.Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData1, 8),
        AddData(inputData2, 8),
        CheckLastBatch((8, 8)),
        StopStream
      )
    }
    val checkpointInfoList = CkptIdCollectingStateStoreWrapper.getStateStoreCheckpointInfos
    // We sometimes add data to both data sources before CheckLastBatch(). They could be picked
    // up by one or two batches. There will be at least 6 batches, but less than 12.
    assert(checkpointInfoList.size % 8 == 0)
    val numBatches = checkpointInfoList.size / 8

    // We don't pass batch versions that would need base checkpoint IDs because we don't know
    // batchIDs for that. We only know that there are 1 batches without it.
    // The two checkpoint IDs in between are stored in the commit log.
    validateCheckpointInfo(numBatches, 4, Set())
    assert(CkptIdCollectingStateStoreWrapper
      .getStateStoreCheckpointInfos
      .count(_.baseStateStoreCkptId.isDefined) == (numBatches - 1) * 8)
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

  test("checkpointFormatVersion2 validate transformWithState") {
    withTempDir { checkpointDir =>
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, Update())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, "a"),
        CheckNewAnswer(("a", "1")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") == 0)
        },
        AddData(inputData, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        StopStream,
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        StopStream,
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
        CheckNewAnswer(("a", "1"), ("c", "1"))
      )
    }
  }
}
