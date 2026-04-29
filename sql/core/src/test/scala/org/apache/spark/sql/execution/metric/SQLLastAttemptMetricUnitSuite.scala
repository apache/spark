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

  test("compact storage: per-component override arrays allocated only when component diverges") {
    val slam = SQLLastAttemptMetrics.createMetric(sc, "test SLAM")
    val acc = slam.copy()

    val rddsMap = lastAttemptRddsMapField.get(slam)
    val mapGetMethod = rddsMap.getClass.getMethod("get", classOf[Object])

    // Establish a common attempt across all 5 mock partitions.
    for (partId <- 0 until 5) {
      setMockAttempt(rddId = 1, partitionId = partId)
      acc.set(10)
      slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    }
    val rddVals = mapGetMethod.invoke(rddsMap, Int.box(1)).asInstanceOf[Option[Object]].get
    val rddValsClass = rddVals.getClass
    // @specialized var fields are emitted with `org$apache$spark$util$LastAttemptRDDVals$$`
    // prefix so specialized subclasses can override them.
    val fieldPrefix = "org$apache$spark$util$LastAttemptRDDVals$$"
    val commonStageIdFld = rddValsClass.getDeclaredField(fieldPrefix + "commonStageId")
    commonStageIdFld.setAccessible(true)
    val commonStageAttemptIdFld =
      rddValsClass.getDeclaredField(fieldPrefix + "commonStageAttemptId")
    commonStageAttemptIdFld.setAccessible(true)
    val commonTaskAttemptNumberFld =
      rddValsClass.getDeclaredField(fieldPrefix + "commonTaskAttemptNumber")
    commonTaskAttemptNumberFld.setAccessible(true)
    val overrideStageIdsFld = rddValsClass.getDeclaredField(fieldPrefix + "overrideStageIds")
    overrideStageIdsFld.setAccessible(true)
    val overrideStageAttemptIdsFld =
      rddValsClass.getDeclaredField(fieldPrefix + "overrideStageAttemptIds")
    overrideStageAttemptIdsFld.setAccessible(true)
    val overrideTaskAttemptNumbersFld =
      rddValsClass.getDeclaredField(fieldPrefix + "overrideTaskAttemptNumbers")
    overrideTaskAttemptNumbersFld.setAccessible(true)

    // No retries: common is set, none of the override arrays are allocated.
    assert(commonStageIdFld.getInt(rddVals) === 10)
    assert(commonStageAttemptIdFld.getInt(rddVals) === 10)
    assert(commonTaskAttemptNumberFld.getInt(rddVals) === 0)
    assert(overrideStageIdsFld.get(rddVals) === null)
    assert(overrideStageAttemptIdsFld.get(rddVals) === null)
    assert(overrideTaskAttemptNumbersFld.get(rddVals) === null)
    assert(slam.lastAttemptValueForRDDId(1) === Some(50))

    // Pure stage-attempt retry of partition 0: only stageAttemptId diverges from the common.
    setMockAttempt(rddId = 1, partitionId = 0)
    acc.set(20)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 11, mockProperties)
    assert(overrideStageIdsFld.get(rddVals) === null,
      "stageId still matches common; its override array should not be allocated")
    assert(overrideStageAttemptIdsFld.get(rddVals) != null,
      "stageAttemptId diverged; its override array should be allocated")
    assert(overrideTaskAttemptNumbersFld.get(rddVals) === null,
      "taskAttemptNumber still matches common; its override array should not be allocated")
    val saIds1 = overrideStageAttemptIdsFld.get(rddVals).asInstanceOf[Array[Int]]
    assert(saIds1.length === 5)
    assert(saIds1(0) === 11)
    assert(saIds1(1) === -1, "Untouched partitions should hold EMPTY_ID (= -1) sentinel")
    assert(slam.lastAttemptValueForRDDId(1) === Some(60)) // 20 + 10*4

    // Mid-stage retry of partition 1: only taskAttemptNumber diverges.
    when(mockTaskInfo.attemptNumber).thenReturn(1)
    when(mockRdd.id).thenReturn(1)
    when(mockTaskInfo.partitionId).thenReturn(1)
    acc.set(15)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 10, 10, mockProperties)
    assert(overrideStageIdsFld.get(rddVals) === null)
    assert(overrideTaskAttemptNumbersFld.get(rddVals) != null,
      "taskAttemptNumber diverged; its override array should be allocated")
    val tans2 = overrideTaskAttemptNumbersFld.get(rddVals).asInstanceOf[Array[Int]]
    assert(tans2(1) === 1)
    assert(tans2(0) === -1)
    assert(slam.lastAttemptValueForRDDId(1) === Some(65)) // 20 + 15 + 10*3

    // Cross-stage retry of partition 2 (new stageId). Now stageId also diverges.
    when(mockTaskInfo.attemptNumber).thenReturn(0)
    when(mockTaskInfo.partitionId).thenReturn(2)
    acc.set(30)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 11, 0, mockProperties)
    assert(overrideStageIdsFld.get(rddVals) != null,
      "stageId now diverges; its override array should be allocated")
    val sIds3 = overrideStageIdsFld.get(rddVals).asInstanceOf[Array[Int]]
    assert(sIds3(2) === 11)
    assert(sIds3(0) === -1)
    assert(slam.lastAttemptValueForRDDId(1) === Some(85)) // 20 + 15 + 30 + 10*2

    // Re-update partition 0 with a value that brings stageAttemptId back to common (10) while
    // diverging stageId (12). Once an override array exists, every update writes its value into
    // the slot - even when the value equals the common - so partition 0's stageAttemptId entry
    // becomes 10 (rather than being cleared to EMPTY_ID).
    when(mockTaskInfo.partitionId).thenReturn(0)
    acc.set(40)
    slam.mergeLastAttempt(acc, mockRdd, mockTaskInfo, 12, 10, mockProperties)
    val saIds4 = overrideStageAttemptIdsFld.get(rddVals).asInstanceOf[Array[Int]]
    assert(saIds4(0) === 10,
      "Partition 0's stageAttemptId entry should hold the new value (which happens to equal " +
        "the common), not EMPTY_ID")
    val sIds4 = overrideStageIdsFld.get(rddVals).asInstanceOf[Array[Int]]
    assert(sIds4(0) === 12)
    assert(slam.lastAttemptValueForRDDId(1) === Some(105)) // 40 + 15 + 30 + 10*2
  }
}
