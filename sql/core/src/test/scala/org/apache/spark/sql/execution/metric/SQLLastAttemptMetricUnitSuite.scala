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
package org.apache.spark.sql.execution.metric

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskInfo

/** Tests internals of [[SQLLastAttemptMetric]]. */
class SQLLastAttemptMetricUnitSuite extends SparkFunSuite with SharedSparkContext {

  // scalastyle:off classforname
  private val sqlLastAttemptMetricClass = Class
    .forName("org.apache.spark.sql.execution.metric.SQLLastAttemptMetric")
  // scalastyle:on classforname

  private val lastAttemptInitializedField =
    sqlLastAttemptMetricClass.getDeclaredField("lastAttemptAccumulatorInitialized")

  private val lastAttemptRddsMapField =
    sqlLastAttemptMetricClass.getDeclaredField(
      "org$apache$spark$util$LastAttemptAccumulator$$lastAttemptRddsMap")

  private val directDriverValueField =
    sqlLastAttemptMetricClass.getDeclaredField(
      "org$apache$spark$util$LastAttemptAccumulator$$lastAttemptDirectDriverValue")

  private val partialMergeValMethod = sqlLastAttemptMetricClass.getMethod("partialMergeVal")

  private val mockRdd = mock[RDD[_]]
  private val mockTaskInfo = mock[TaskInfo]
  private val mockProperties = new Properties

  // Set mock attempt for mock Task, TaskInfo and RDD
  // that can be used with mergeLastAttempt.
  // stageId and stageAttemptId are passed directly to mergeLastAttempt.
  def setMockAttempt(rddId: Int, partitionId: Int): Unit = {
    // reset to mock defaults
    when(mockTaskInfo.attemptNumber).thenReturn(0)
    when(mockRdd.scope).thenReturn(None)
    when(mockRdd.getNumPartitions).thenReturn(5)

    when(mockRdd.id).thenReturn(rddId)
    when(mockTaskInfo.partitionId).thenReturn(partitionId)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    lastAttemptInitializedField.setAccessible(true)
    lastAttemptRddsMapField.setAccessible(true)
    directDriverValueField.setAccessible(true)
    partialMergeValMethod.setAccessible(true)
  }

  override def afterAll(): Unit = {
    lastAttemptInitializedField.setAccessible(false)
    lastAttemptRddsMapField.setAccessible(false)
    directDriverValueField.setAccessible(false)
    partialMergeValMethod.setAccessible(false)
    super.afterAll()
  }

  test("serialization and deserialization") {
    val slam = SQLLastAttemptMetrics.createMetric(sc, "test SLAM")

    assert(lastAttemptInitializedField.getBoolean(slam) === true)
    assert(lastAttemptRddsMapField.get(slam) != null)
    assert(directDriverValueField.get(slam) != null)

    // Serialize slam to ObjectOutputStream and deserialize it back.
    val obs1 = new ByteArrayOutputStream()
    val oos1 = new ObjectOutputStream(obs1)
    oos1.writeObject(slam)
    oos1.close()
    val ois1 = new ObjectInputStream(new ByteArrayInputStream(obs1.toByteArray))
    val deser = ois1.readObject().asInstanceOf[SQLLastAttemptMetric]

    // serialized version should not be initialized
    assert(lastAttemptInitializedField.getBoolean(deser) === false)
    assert(lastAttemptRddsMapField.get(deser) == null)
    assert(directDriverValueField.get(deser) == null)

    deser.set(42)
    deser.add(7)
    assert(deser.value === 49)
    // these functions shouldn't be used on the deserialized metric,
    // but assertions should be caught and None should be returned.
    assert(deser.lastAttemptValueForHighestRDDId() === None)
    assert(deser.lastAttemptValueForRDDId(1) === None)
    assert(deser.lastAttemptValueForRDDIds(Seq(1, 2, 3)) === None)
    assert(deser.lastAttemptValueForAllRDDs() === None)
    // mergeLastAttempt shouldn't be used on the deserialized metric,
    // but it should catch error and not fail.
    deser.mergeLastAttempt(slam, null, null, 0, 0, null)

    // Serialize and deserialize again.
    val obs2 = new ByteArrayOutputStream()
    val oos2 = new ObjectOutputStream(obs2)
    oos2.writeObject(deser)
    oos2.close()
    val ois2 = new ObjectInputStream(new ByteArrayInputStream(obs2.toByteArray))
    val reser = ois2.readObject().asInstanceOf[SQLLastAttemptMetric]
    // Check that the value is brought back and can be used as partialMergeVal.
    assert(reser.value === 49L)
    assert(partialMergeValMethod.invoke(reser) === 49L)
  }

  test("copy and mergeLastAttempt") {
    val slam = SQLLastAttemptMetrics.createMetric(sc, "test SLAM")

    assert(lastAttemptInitializedField.getBoolean(slam) == true)
    assert(lastAttemptRddsMapField.get(slam) != null)
    assert(directDriverValueField.get(slam) != null)

    // copy should not initialize SLAM data.
    val acc = slam.copy()
    assert(lastAttemptInitializedField.getBoolean(acc) == false)
    assert(lastAttemptRddsMapField.get(acc) == null)
    assert(directDriverValueField.get(acc) == null)
    // these functions shouldn't be used on the copy,
    // but assertions should be caught and None should be returned.
    assert(acc.lastAttemptValueForHighestRDDId() === None)
    assert(acc.lastAttemptValueForRDDId(1) === None)
    assert(acc.lastAttemptValueForRDDIds(Seq(1, 2, 3)) === None)
    assert(acc.lastAttemptValueForAllRDDs() === None)
    // mergeLastAttempt shouldn't be used on the copy,
    // but it should catch error and not fail.
    acc.mergeLastAttempt(slam, null, null, 0, 0, null)

    // Let's play with merging acc into slam.
    setMockAttempt(rddId = 1, partitionId = 0)
    acc.set(10)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(10))

    setMockAttempt(rddId = 1, partitionId = 1)
    acc.set(10) // new partition id
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(20)) // 10 + 10, aggregated new partition id

    setMockAttempt(rddId = 1, partitionId = 1)
    acc.set(7) // same partition id, older attempt.
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 9, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(20)) // no change

    setMockAttempt(rddId = 1, partitionId = 1)
    acc.set(7) // same partition id, older stage.
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 9, 11, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(20)) // no change

    setMockAttempt(rddId = 1, partitionId = 1)
    acc.set(7) // same partition id, newer attempt.
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(17)) // 10 replaced with 7

    setMockAttempt(rddId = 1, partitionId = 1)
    acc.set(8) // same partition id, newer stage.
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 11, 1, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(18)) // 7 replaced with 8

    setMockAttempt(rddId = 2, partitionId = 2)
    acc.set(42) // new RDD
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 1, 1, mockProperties)
    assert(slam.lastAttemptValueForRDDId(1) === Some(18)) // no change for rddId=1
    assert(slam.lastAttemptValueForRDDId(2) === Some(42)) // new RDD added
    assert(slam.lastAttemptValueForAllRDDs() === Some(60))
  }

  test("compact storage: common attempt shared, overrides only for retries") {
    val slam = SQLLastAttemptMetrics.createMetric(sc, "test SLAM")
    val acc = slam.copy()

    val rddsMap = lastAttemptRddsMapField.get(slam)
    val mapGetMethod = rddsMap.getClass.getMethod("get", classOf[Object])

    def rddValsForRddId(rddId: Int): Object = {
      val opt = mapGetMethod.invoke(rddsMap, Int.box(rddId)).asInstanceOf[Option[Object]]
      opt.get
    }

    // First update establishes the common attempt; subsequent updates on the same attempt
    // record only their value and the bitmap bit, no overrides.
    for (partId <- 0 until 5) {
      setMockAttempt(rddId = 1, partitionId = partId)
      acc.set(10)
      slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    }
    val rddVals = rddValsForRddId(1)
    val rddValsClass = rddVals.getClass
    // @specialized var fields are emitted with `org$apache$spark$util$LastAttemptRDDVals$$`
    // prefix so specialized subclasses can override them.
    val fieldPrefix = "org$apache$spark$util$LastAttemptRDDVals$$"
    val overrideSizeFld = rddValsClass.getDeclaredField(fieldPrefix + "overrideSize")
    overrideSizeFld.setAccessible(true)
    val commonStageIdFld = rddValsClass.getDeclaredField(fieldPrefix + "commonStageId")
    commonStageIdFld.setAccessible(true)
    val commonStageAttemptIdFld =
      rddValsClass.getDeclaredField(fieldPrefix + "commonStageAttemptId")
    commonStageAttemptIdFld.setAccessible(true)
    val commonTaskAttemptNumberFld =
      rddValsClass.getDeclaredField(fieldPrefix + "commonTaskAttemptNumber")
    commonTaskAttemptNumberFld.setAccessible(true)

    assert(overrideSizeFld.getInt(rddVals) === 0,
      "Common case (no retries) should not allocate any overrides")
    assert(commonStageIdFld.getInt(rddVals) === 10)
    assert(commonStageAttemptIdFld.getInt(rddVals) === 10)
    assert(commonTaskAttemptNumberFld.getInt(rddVals) === 0)
    assert(slam.lastAttemptValueForRDDId(1) === Some(50))

    // A retry of one partition with a newer stageAttemptId records exactly one override
    // entry; common values are untouched.
    setMockAttempt(rddId = 1, partitionId = 0)
    acc.set(20)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === 1)
    assert(commonStageIdFld.getInt(rddVals) === 10)
    assert(commonStageAttemptIdFld.getInt(rddVals) === 10)
    assert(slam.lastAttemptValueForRDDId(1) === Some(60)) // 20 + 10*4

    // Re-retrying the same partition with a newer attempt updates the override entry
    // in place; the override size does not grow.
    setMockAttempt(rddId = 1, partitionId = 0)
    acc.set(30)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 11, 0, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === 1)
    assert(slam.lastAttemptValueForRDDId(1) === Some(70)) // 30 + 10*4

    // A different partition retried adds a second override entry.
    setMockAttempt(rddId = 1, partitionId = 2)
    acc.set(15)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === 2)
    assert(slam.lastAttemptValueForRDDId(1) === Some(75)) // 30 + 10 + 15 + 10 + 10

    // An update with a per-task retry (different taskAttemptNumber, same stage) is also
    // recorded as an override.
    when(mockTaskInfo.attemptNumber).thenReturn(1)
    when(mockRdd.id).thenReturn(1)
    when(mockTaskInfo.partitionId).thenReturn(4)
    acc.set(8)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === 3)
    assert(slam.lastAttemptValueForRDDId(1) === Some(73)) // 30 + 10 + 15 + 10 + 8
  }

  test("compact storage: override index map built lazily once threshold is crossed") {
    val slam = SQLLastAttemptMetrics.createMetric(sc, "test SLAM")
    val acc = slam.copy()

    // Read the threshold from the companion object so this test stays in sync if the constant
    // is tuned.
    // scalastyle:off classforname
    val companion = Class.forName("org.apache.spark.util.LastAttemptRDDVals$")
      .getField("MODULE$").get(null)
    // scalastyle:on classforname
    val threshold = companion.getClass.getMethod("OverrideIdxMapThreshold")
      .invoke(companion).asInstanceOf[Int]

    val numParts = threshold + 20 // headroom above and below the threshold
    when(mockTaskInfo.attemptNumber).thenReturn(0)
    when(mockRdd.scope).thenReturn(None)
    when(mockRdd.getNumPartitions).thenReturn(numParts)
    when(mockRdd.id).thenReturn(7)

    // Establish a common attempt across all partitions; no overrides yet.
    for (partId <- 0 until numParts) {
      when(mockTaskInfo.partitionId).thenReturn(partId)
      acc.set(1)
      slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    }

    val rddsMap = lastAttemptRddsMapField.get(slam)
    val mapGetMethod = rddsMap.getClass.getMethod("get", classOf[Object])
    val rddVals = mapGetMethod.invoke(rddsMap, Int.box(7)).asInstanceOf[Option[Object]].get
    val rddValsClass = rddVals.getClass
    val fieldPrefix = "org$apache$spark$util$LastAttemptRDDVals$$"
    val overrideSizeFld = rddValsClass.getDeclaredField(fieldPrefix + "overrideSize")
    overrideSizeFld.setAccessible(true)
    val overrideIdxMapFld = rddValsClass.getDeclaredField(fieldPrefix + "overrideIdxMap")
    overrideIdxMapFld.setAccessible(true)

    assert(overrideSizeFld.getInt(rddVals) === 0)
    assert(overrideIdxMapFld.get(rddVals) === null,
      "Map must not be allocated when there are no overrides")

    // Retry the first `threshold` partitions with a different stageAttemptId; we sit exactly at
    // the threshold, so the map should still be null.
    for (partId <- 0 until threshold) {
      when(mockTaskInfo.partitionId).thenReturn(partId)
      acc.set(2)
      slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    }
    assert(overrideSizeFld.getInt(rddVals) === threshold)
    assert(overrideIdxMapFld.get(rddVals) === null,
      s"At threshold ($threshold overrides) the map should still be null")

    // The next override crosses the threshold; the map is built and populated with all entries.
    when(mockTaskInfo.partitionId).thenReturn(threshold)
    acc.set(3)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === threshold + 1)
    val mapAfterThreshold = overrideIdxMapFld.get(rddVals)
      .asInstanceOf[scala.collection.mutable.HashMap[Int, Int]]
    assert(mapAfterThreshold != null, "Map must be allocated after crossing the threshold")
    assert(mapAfterThreshold.size === threshold + 1)
    for (partId <- 0 to threshold) {
      assert(mapAfterThreshold.contains(partId),
        s"Map must contain partition $partId after the threshold crossing")
    }

    // A new override appended after the threshold extends the existing map.
    when(mockTaskInfo.partitionId).thenReturn(threshold + 1)
    acc.set(4)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === threshold + 2)
    assert(mapAfterThreshold.size === threshold + 2)
    assert(mapAfterThreshold.contains(threshold + 1))

    // An in-place override update (newer attempt for an existing partition) leaves the map
    // unchanged - the index doesn't move.
    when(mockTaskInfo.partitionId).thenReturn(0)
    val mapEntryBefore = mapAfterThreshold(0)
    acc.set(5)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 11, 0, mockProperties)
    assert(overrideSizeFld.getInt(rddVals) === threshold + 2)
    assert(mapAfterThreshold.size === threshold + 2)
    assert(mapAfterThreshold(0) === mapEntryBefore,
      "In-place override update must not move the index")

    // The aggregate is: (numParts - threshold - 2) partitions still on the common attempt
    // contribute 1; partitions [1, threshold-1] are overridden with value 2; partition
    // `threshold` contributes 3; partition `threshold + 1` contributes 4; partition 0
    // contributes 5 (its latest override).
    val expected =
      (numParts - threshold - 2) * 1 + (threshold - 1) * 2 + 1 * 3 + 1 * 4 + 1 * 5
    assert(slam.lastAttemptValueForRDDId(7) === Some(expected))
  }
}
