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

package org.apache.spark.sql.execution

import org.apache.spark.{MapOutputStatistics, SparkFunSuite}
import org.apache.spark.sql.execution.adaptive.ShufflePartitionsCoalescer

class ShufflePartitionsCoalescerSuite extends SparkFunSuite {

  private def checkEstimation(
      bytesByPartitionIdArray: Array[Array[Long]],
      expectedPartitionStartIndices: Array[Int],
      targetSize: Long,
      minNumPartitions: Int = 1): Unit = {
    val mapOutputStatistics = bytesByPartitionIdArray.zipWithIndex.map {
      case (bytesByPartitionId, index) =>
        new MapOutputStatistics(index, bytesByPartitionId)
    }
    val estimatedPartitionStartIndices = ShufflePartitionsCoalescer.coalescePartitions(
      mapOutputStatistics,
      0,
      bytesByPartitionIdArray.head.length,
      targetSize,
      minNumPartitions)
    assert(estimatedPartitionStartIndices === expectedPartitionStartIndices)
  }

  test("1 shuffle") {
    val targetSize = 100

    {
      // All bytes per partition are 0.
      val bytesByPartitionId = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(Array(bytesByPartitionId), expectedPartitionStartIndices, targetSize)
    }

    {
      // Some bytes per partition are 0 and total size is less than the target size.
      // 1 coalesced partition is expected.
      val bytesByPartitionId = Array[Long](10, 0, 20, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(Array(bytesByPartitionId), expectedPartitionStartIndices, targetSize)
    }

    {
      // 2 coalesced partitions are expected.
      val bytesByPartitionId = Array[Long](10, 0, 90, 20, 0)
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(Array(bytesByPartitionId), expectedPartitionStartIndices, targetSize)
    }

    {
      // There are a few large shuffle partitions.
      val bytesByPartitionId = Array[Long](110, 10, 100, 110, 0)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(Array(bytesByPartitionId), expectedPartitionStartIndices, targetSize)
    }

    {
      // All shuffle partitions are larger than the targeted size.
      val bytesByPartitionId = Array[Long](100, 110, 100, 110, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(Array(bytesByPartitionId), expectedPartitionStartIndices, targetSize)
    }

    {
      // The last shuffle partition is in a single coalesced partition.
      val bytesByPartitionId = Array[Long](30, 30, 0, 40, 110)
      val expectedPartitionStartIndices = Array[Int](0, 4)
      checkEstimation(Array(bytesByPartitionId), expectedPartitionStartIndices, targetSize)
    }
  }

  test("2 shuffles") {
    val targetSize = 100

    {
      // If there are multiple values of the number of shuffle partitions,
      // we should see an assertion error.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0, 0)
      intercept[AssertionError] {
        checkEstimation(Array(bytesByPartitionId1, bytesByPartitionId2), Array.empty, targetSize)
      }
    }

    {
      // All bytes per partition are 0.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }

    {
      // Some bytes per partition are 0.
      // 1 coalesced partition is expected.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 20, 0, 20)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }

    {
      // 2 coalesced partition are expected.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 2, 4)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }

    {
      // 4 coalesced partition are expected.
      val bytesByPartitionId1 = Array[Long](0, 99, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 4)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }

    {
      // 2 coalesced partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 100, 0, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 4)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }

    {
      // There are a few large shuffle partitions.
      val bytesByPartitionId1 = Array[Long](0, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 0, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }

    {
      // All pairs of shuffle partitions are larger than the targeted size.
      val bytesByPartitionId1 = Array[Long](100, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 70, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize)
    }
  }

  test("enforce minimal number of coalesced partitions") {
    val targetSize = 100
    val minNumPartitions = 2

    {
      // The minimal number of coalesced partitions is not enforced because
      // the size of data is 0.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize, minNumPartitions)
    }

    {
      // The minimal number of coalesced partitions is enforced.
      val bytesByPartitionId1 = Array[Long](10, 5, 5, 0, 20)
      val bytesByPartitionId2 = Array[Long](5, 10, 0, 10, 5)
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize, minNumPartitions)
    }

    {
      // The number of coalesced partitions is determined by the algorithm.
      val bytesByPartitionId1 = Array[Long](10, 50, 20, 80, 20)
      val bytesByPartitionId2 = Array[Long](40, 10, 0, 10, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 3, 4)
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices,
        targetSize, minNumPartitions)
    }
  }
}
