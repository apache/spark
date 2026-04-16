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
}
