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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.softaffinity.{ExecutorsListListener, SoftAffinityConstants, SoftAffinityManager}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class FileScanRDDWithSoftAffinitySuite extends QueryTest
  with SharedSparkSession with PredicateHelper {

  protected override def sparkConf = super.sparkConf
    .set(SoftAffinityConstants.PARAMS_KEY_ENABLED_SOFT_AFFINITY, "true")
    .set(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_REPLICATION_NUM, "2")
    .set(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_MIN_TARGET_HOSTS, "2")

  def generateFileScanRDD1(): Unit = {
    val partition = FilePartition(0, Seq(
      PartitionedFile(InternalRow.empty, "fakePath0", 0, 100, Array("host-1", "host-2")),
      PartitionedFile(InternalRow.empty, "fakePath1", 0, 200, Array("host-2", "host-3"))
    ).toArray)

    val fakeRDD = new FileScanRDD(
      spark,
      (file: PartitionedFile) => Iterator.empty,
      Seq(partition),
      StructType(Seq.empty)
    )

    assertResult(Set("host-1", "host-2", "host-3")) {
      fakeRDD.preferredLocations(partition).toSet
    }
  }

  def generateFileScanRDD2(): Unit = {
    val partition = FilePartition(0, Seq(
      PartitionedFile(InternalRow.empty, "fakePath0", 0, 100, Array("host-1", "host-2")),
      PartitionedFile(InternalRow.empty, "fakePath1", 0, 200, Array("host-4", "host-5"))
    ).toArray)

    val fakeRDD = new FileScanRDD(
      spark,
      (file: PartitionedFile) => Iterator.empty,
      Seq(partition),
      StructType(Seq.empty)
    )

    assertResult(Set("host-1", "host-4", "host-5")) {
      fakeRDD.preferredLocations(partition).toSet
    }
  }

  def generateFileScanRDD3(): Unit = {
    val partition = FilePartition(0, Seq(
      PartitionedFile(InternalRow.empty, "fakePath0", 0, 100, Array("host-1", "host-2")),
      PartitionedFile(InternalRow.empty, "fakePath1", 0, 200, Array("host-5", "host-6"))
    ).toArray)

    val fakeRDD = new FileScanRDD(
      spark,
      (file: PartitionedFile) => Iterator.empty,
      Seq(partition),
      StructType(Seq.empty)
    )

    assertResult(Set("executor_host-2_2", "executor_host-3_4")) {
      fakeRDD.preferredLocations(partition).toSet
    }
  }

  def generateFileScanRDD4(): Unit = {
    val partition = FilePartition(0, Seq(
      PartitionedFile(InternalRow.empty, "fakePath0", 0, 100, Array("host-1", "host-2")),
      PartitionedFile(InternalRow.empty, "fakePath1", 0, 200, Array("host-5", "host-6"))
    ).toArray)

    val fakeRDD = new FileScanRDD(
      spark,
      (file: PartitionedFile) => Iterator.empty,
      Seq(partition),
      StructType(Seq.empty)
    )

    assertResult(Set("host-1", "host-5", "host-6")) {
      fakeRDD.preferredLocations(partition).toSet
    }
  }

  def generateFileScanRDD5(): Unit = {
    val partition = FilePartition(0, Seq(
      PartitionedFile(InternalRow.empty, "fakePath0", 0, 100, Array("host-1", "host-2")),
      PartitionedFile(InternalRow.empty, "fakePath1", 0, 200, Array("host-5", "host-6"))
    ).toArray)

    val fakeRDD = new FileScanRDD(
      spark,
      (file: PartitionedFile) => Iterator.empty,
      Seq(partition),
      StructType(Seq.empty)
    )

    assertResult(Set("host-1", "host-5", "host-6")) {
      fakeRDD.preferredLocations(partition).toSet
    }
  }

  test("Soft Affinity Scheduler for FileScanRDD") {

    val addEvent0 = SparkListenerExecutorAdded(System.currentTimeMillis(), "0",
      new ExecutorInfo("host-1", 3, null))
    val addEvent1 = SparkListenerExecutorAdded(System.currentTimeMillis(), "1",
      new ExecutorInfo("host-1", 3, null))
    val addEvent2 = SparkListenerExecutorAdded(System.currentTimeMillis(), "2",
      new ExecutorInfo("host-2", 3, null))
    val addEvent3 = SparkListenerExecutorAdded(System.currentTimeMillis(), "3",
      new ExecutorInfo("host-3", 3, null))
    val addEvent3_1 = SparkListenerExecutorAdded(System.currentTimeMillis(), "3",
      new ExecutorInfo("host-5", 3, null))
    val addEvent4 = SparkListenerExecutorAdded(System.currentTimeMillis(), "4",
      new ExecutorInfo("host-3", 3, null))
    val addEvent5 = SparkListenerExecutorAdded(System.currentTimeMillis(), "5",
      new ExecutorInfo("host-2", 3, null))
    val addEvent6 = SparkListenerExecutorAdded(System.currentTimeMillis(), "6",
      new ExecutorInfo("host-4", 3, null))

    val removedEvent0 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "0", "")
    val removedEvent1 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "1", "")
    val removedEvent2 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "2", "")
    val removedEvent3 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "3", "")
    val removedEvent3_1 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "3", "")
    val removedEvent4 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "4", "")
    val removedEvent5 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "5", "")
    val removedEvent6 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "6", "")
    val removedEvent7 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "7", "")

    val executorsListListener = new ExecutorsListListener()

    executorsListListener.onExecutorAdded(addEvent0)
    executorsListListener.onExecutorAdded(addEvent1)
    executorsListListener.onExecutorAdded(addEvent2)
    executorsListListener.onExecutorAdded(addEvent3)
    // test adding executor repeatedly
    executorsListListener.onExecutorAdded(addEvent3_1)

    assert(SoftAffinityManager.nodesExecutorsMap.size == 3)
    assert(SoftAffinityManager.fixedIdForExecutors.size == 4)

    executorsListListener.onExecutorRemoved(removedEvent3)
    // test removing executor repeatedly
    executorsListListener.onExecutorRemoved(removedEvent3_1)

    assert(SoftAffinityManager.nodesExecutorsMap.size == 2)
    assert(SoftAffinityManager.fixedIdForExecutors.size == 4)
    assert(SoftAffinityManager.fixedIdForExecutors.exists(_.isEmpty))

    executorsListListener.onExecutorAdded(addEvent4)
    executorsListListener.onExecutorAdded(addEvent5)
    executorsListListener.onExecutorAdded(addEvent6)

    assert(SoftAffinityManager.nodesExecutorsMap.size == 4)
    assert(SoftAffinityManager.fixedIdForExecutors.size == 6)
    assert(!SoftAffinityManager.fixedIdForExecutors.exists(_.isEmpty))

    // all target hosts exist in computing hosts list, return the original hosts list
    generateFileScanRDD1()
    // there are two target hosts existing in computing hosts list, return the original hosts list
    generateFileScanRDD2()
    // there are only one target host existing in computing hosts list,
    // return the hash executors list
    generateFileScanRDD3()

    executorsListListener.onExecutorRemoved(removedEvent2)
    executorsListListener.onExecutorRemoved(removedEvent4)

    assert(SoftAffinityManager.nodesExecutorsMap.size == 3)
    assert(SoftAffinityManager.fixedIdForExecutors.size == 6)
    assert(SoftAffinityManager.fixedIdForExecutors.exists(_.isEmpty))

    // there are only one target host existing in computing hosts list,
    // but the hash executors were removed, so return the original hosts list
    generateFileScanRDD4()

    executorsListListener.onExecutorRemoved(removedEvent0)
    executorsListListener.onExecutorRemoved(removedEvent1)
    executorsListListener.onExecutorRemoved(removedEvent5)
    executorsListListener.onExecutorRemoved(removedEvent6)
    executorsListListener.onExecutorRemoved(removedEvent7)

    assert(SoftAffinityManager.nodesExecutorsMap.size == 0)
    assert(SoftAffinityManager.fixedIdForExecutors.size == 6)
    assert(SoftAffinityManager.fixedIdForExecutors.exists(_.isEmpty))

    // all executors were removed, return the original hosts list
    generateFileScanRDD5()
  }
}
