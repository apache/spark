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

import org.apache.spark.{LocalSparkContext, MapOutputStatistics, MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil
import org.apache.spark.storage.BlockManagerId

class ShufflePartitionsUtilSuite extends SparkFunSuite with LocalSparkContext {

  private def checkEstimation(
      bytesByPartitionIdArray: Array[Array[Long]],
      expectedPartitionStartIndices: Seq[Seq[CoalescedPartitionSpec]],
      targetSize: Long,
      minNumPartitions: Int = 1,
      minPartitionSize: Long = 0): Unit = {
    val mapOutputStatistics = bytesByPartitionIdArray.zipWithIndex.map {
      case (bytesByPartitionId, index) =>
        Some(new MapOutputStatistics(index, bytesByPartitionId))
    }
    val estimatedPartitionStartIndices = ShufflePartitionsUtil.coalescePartitions(
      mapOutputStatistics,
      Seq.fill(mapOutputStatistics.length)(None),
      targetSize,
      minNumPartitions,
      minPartitionSize)
    assert(estimatedPartitionStartIndices.length === expectedPartitionStartIndices.length)
    estimatedPartitionStartIndices.zip(expectedPartitionStartIndices).foreach {
      case (actual, expect) => assert(actual === expect)
    }
  }

  test("1 shuffle") {
    val targetSize = 100

    {
      // Some bytes per partition are 0 and total size is less than the target size.
      // 1 coalesced partition is expected.
      val bytesByPartitionId = Array[Long](10, 0, 20, 0, 0)
      val expectedPartitionSpecs = Seq(CoalescedPartitionSpec(0, 5, 30))
      checkEstimation(Array(bytesByPartitionId), expectedPartitionSpecs :: Nil, targetSize)
    }

    {
      // 2 coalesced partitions are expected.
      val bytesByPartitionId = Array[Long](10, 0, 90, 20, 0)
      val expectedPartitionSpecs =
        Seq(CoalescedPartitionSpec(0, 3, 100), CoalescedPartitionSpec(3, 5, 20))
      checkEstimation(Array(bytesByPartitionId), expectedPartitionSpecs :: Nil, targetSize)
    }

    {
      // There are a few large shuffle partitions.
      val bytesByPartitionId = Array[Long](110, 10, 100, 110, 0)
      val expectedPartitionSpecs = Seq(
        CoalescedPartitionSpec(0, 1, 110),
        CoalescedPartitionSpec(1, 2, 10),
        CoalescedPartitionSpec(2, 3, 100),
        CoalescedPartitionSpec(3, 4, 110))
      checkEstimation(Array(bytesByPartitionId), expectedPartitionSpecs :: Nil, targetSize)
    }

    {
      // All shuffle partitions are larger than the targeted size.
      // return Nil if cannot coalesce
      val bytesByPartitionId = Array[Long](100, 110, 100, 110, 110)
      checkEstimation(Array(bytesByPartitionId), Nil, targetSize)
    }

    {
      // The last shuffle partition is in a single coalesced partition.
      val bytesByPartitionId = Array[Long](30, 30, 0, 40, 110)
      val expectedPartitionSpecs =
        Seq(CoalescedPartitionSpec(0, 4, 100), CoalescedPartitionSpec(4, 5, 110))
      checkEstimation(Array(bytesByPartitionId), expectedPartitionSpecs :: Nil, targetSize)
    }
  }

  test("2 shuffles") {
    val targetSize = 100

    {
      // If there are multiple values of the number of shuffle partitions,
      // we should return empty coalesced result.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0, 0)
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq.fill(2)(None), targetSize, 1, 0)
      assert(coalesced.isEmpty)
    }

    {
      // Some bytes per partition are 0.
      // 1 coalesced partition is expected.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 20, 0, 20)
      val expectedPartitionSpecs1 = Seq(CoalescedPartitionSpec(0, 5, 30))
      val expectedPartitionSpecs2 = Seq(CoalescedPartitionSpec(0, 5, 70))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize)
    }

    {
      // 2 coalesced partition are expected.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionSpecs1 = Seq(
        CoalescedPartitionSpec(0, 2, 10),
        CoalescedPartitionSpec(2, 4, 20),
        CoalescedPartitionSpec(4, 5, 0))
      val expectedPartitionSpecs2 = Seq(
        CoalescedPartitionSpec(0, 2, 30),
        CoalescedPartitionSpec(2, 4, 70),
        CoalescedPartitionSpec(4, 5, 30))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize)
    }

    {
      // 4 coalesced partition are expected.
      val bytesByPartitionId1 = Array[Long](0, 99, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionSpecs1 = Seq(
        CoalescedPartitionSpec(0, 1, 0),
        CoalescedPartitionSpec(1, 2, 99),
        CoalescedPartitionSpec(2, 4, 20),
        CoalescedPartitionSpec(4, 5, 0))
      val expectedPartitionSpecs2 = Seq(
        CoalescedPartitionSpec(0, 1, 30),
        CoalescedPartitionSpec(1, 2, 0),
        CoalescedPartitionSpec(2, 4, 70),
        CoalescedPartitionSpec(4, 5, 30))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize)
    }

    {
      // 2 coalesced partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 100, 0, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionSpecs1 = Seq(
        CoalescedPartitionSpec(0, 1, 0),
        CoalescedPartitionSpec(1, 2, 100),
        CoalescedPartitionSpec(2, 4, 30),
        CoalescedPartitionSpec(4, 5, 0))
      val expectedPartitionSpecs2 = Seq(
        CoalescedPartitionSpec(0, 1, 30),
        CoalescedPartitionSpec(1, 2, 0),
        CoalescedPartitionSpec(2, 4, 70),
        CoalescedPartitionSpec(4, 5, 30))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize)
    }

    {
      // There are a few large shuffle partitions.
      // return Nil if cannot coalesce
      val bytesByPartitionId1 = Array[Long](0, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 0, 110)
      checkEstimation(Array(bytesByPartitionId1, bytesByPartitionId2), Nil, targetSize)
    }

    {
      // All pairs of shuffle partitions are larger than the targeted size.
      // return Nil if cannot coalesce
      val bytesByPartitionId1 = Array[Long](100, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 70, 110)
      checkEstimation(Array(bytesByPartitionId1, bytesByPartitionId2), Nil, targetSize)
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
      // Create at least one partition spec
      val expectedPartitionSpecs1 = Seq(CoalescedPartitionSpec(0, 5, 0))
      val expectedPartitionSpecs2 = Seq(CoalescedPartitionSpec(0, 5, 0))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2), targetSize, minNumPartitions)
    }

    {
      // The minimal number of coalesced partitions is not enforced because
      // there are too many 0-size partitions.
      val bytesByPartitionId1 = Array[Long](200, 0, 0)
      val bytesByPartitionId2 = Array[Long](100, 0, 0)
      val expectedPartitionSpecs1 = Seq(CoalescedPartitionSpec(0, 1, 200))
      val expectedPartitionSpecs2 = Seq(CoalescedPartitionSpec(0, 1, 100))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize, minNumPartitions)
    }

    {
      // The minimal number of coalesced partitions is enforced.
      val bytesByPartitionId1 = Array[Long](10, 5, 5, 0, 20)
      val bytesByPartitionId2 = Array[Long](5, 10, 0, 10, 5)
      val expectedPartitionSpecs1 =
        Seq(CoalescedPartitionSpec(0, 3, 20), CoalescedPartitionSpec(3, 5, 20))
      val expectedPartitionSpecs2 =
        Seq(CoalescedPartitionSpec(0, 3, 15), CoalescedPartitionSpec(3, 5, 15))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize, minNumPartitions)
    }

    {
      // The number of coalesced partitions is determined by the algorithm.
      val bytesByPartitionId1 = Array[Long](10, 50, 20, 80, 20)
      val bytesByPartitionId2 = Array[Long](40, 10, 0, 10, 30)
      val expectedPartitionSpecs1 = Seq(
        CoalescedPartitionSpec(0, 1, 10),
        CoalescedPartitionSpec(1, 3, 70),
        CoalescedPartitionSpec(3, 4, 80),
        CoalescedPartitionSpec(4, 5, 20))
      val expectedPartitionSpecs2 = Seq(
        CoalescedPartitionSpec(0, 1, 40),
        CoalescedPartitionSpec(1, 3, 10),
        CoalescedPartitionSpec(3, 4, 10),
        CoalescedPartitionSpec(4, 5, 30))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize, minNumPartitions)
    }
  }

  test("do not create partition spec for 0-size partitions") {
    val targetSize = 100
    val minNumPartitions = 2

    {
      // 1 shuffle: All bytes per partition are 0, 1 empty partition spec created.
      val bytesByPartitionId = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionSpecs = Seq(CoalescedPartitionSpec(0, 5, 0))
      checkEstimation(Array(bytesByPartitionId), expectedPartitionSpecs :: Nil, targetSize)
    }

    {
      // 2 shuffles: All bytes per partition are 0, 1 empty partition spec created.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionSpecs1 = Seq(CoalescedPartitionSpec(0, 5, 0))
      val expectedPartitionSpecs2 = Seq(CoalescedPartitionSpec(0, 5, 0))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize)
    }

    {
      // No partition spec created for the 0-size partitions.
      val bytesByPartitionId1 = Array[Long](200, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](100, 0, 300, 0, 0)
      val expectedPartitionSpecs1 = Seq(
        CoalescedPartitionSpec(0, 1, 200),
        CoalescedPartitionSpec(2, 3, 0))
      val expectedPartitionSpecs2 = Seq(
        CoalescedPartitionSpec(0, 1, 100),
        CoalescedPartitionSpec(2, 3, 300))
      checkEstimation(
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Seq(expectedPartitionSpecs1, expectedPartitionSpecs2),
        targetSize, minNumPartitions)
    }
  }

  test("coalesce after skew splitting") {
    val targetSize = 100

    def createPartitionSpecsBeforeCoalesce(
        range: Range, bytes: Array[Long]): Seq[CoalescedPartitionSpec] = {
      range.map(i => CoalescedPartitionSpec(i, i + 1, bytes(i)))
    }

    {
      // Skew sections in the middle.
      val bytesByPartitionId1 = Array[Long](10, 5, 300, 10, 8, 10, 10, 20)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 8, 10, 200, 7, 20)
      val specs1 =
        createPartitionSpecsBeforeCoalesce(0 to 1, bytesByPartitionId1) ++ // 0, 1
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(2, i, i + 1, 100L)) ++ // 2 - skew
        createPartitionSpecsBeforeCoalesce(3 to 4, bytesByPartitionId1) ++ // 3, 4
        Seq.fill(2)(CoalescedPartitionSpec(5, 6, 10)) ++ // 5 - other side skew
        createPartitionSpecsBeforeCoalesce(6 to 7, bytesByPartitionId1) // 6, 7
      val specs2 =
        createPartitionSpecsBeforeCoalesce(0 to 1, bytesByPartitionId2) ++ // 0, 1
        Seq.fill(3)(CoalescedPartitionSpec(2, 3, 10)) ++ // 2 - other side skew
        createPartitionSpecsBeforeCoalesce(3 to 4, bytesByPartitionId2) ++ // 3, 4
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(5, i, i + 1, 100L)) ++ // 5 - skew
        createPartitionSpecsBeforeCoalesce(6 to 7, bytesByPartitionId2) // 6, 7
      val expected1 =
        Seq(CoalescedPartitionSpec(0, 2, 15)) ++ // 0, 1 - coalesced
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(2, i, i + 1, 100L)) ++ // 2 - skew
        Seq(CoalescedPartitionSpec(3, 5, 18)) ++ // 3, 4 - coalesced
        Seq.fill(2)(CoalescedPartitionSpec(5, 6, 10)) ++ // 5 - other side skew
        Seq(CoalescedPartitionSpec(6, 8, 30)) // 6, 7 - coalesced
      val expected2 =
        Seq(CoalescedPartitionSpec(0, 2, 20)) ++ // 0, 1 - coalesced
        Seq.fill(3)(CoalescedPartitionSpec(2, 3, 10)) ++ // 2 - other side skew
        Seq(CoalescedPartitionSpec(3, 5, 18)) ++ // 3, 4 - coalesced
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(5, i, i + 1, 100L)) ++ // 5 - skew
        Seq(CoalescedPartitionSpec(6, 8, 27)) // 6, 7 - coalesced
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, 0)
      assert(coalesced == Seq(expected1, expected2))
    }

    {
      // Skew sections at both ends.
      val bytesByPartitionId1 = Array[Long](300, 5, 10, 10, 8, 10, 10, 20)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 8, 10, 20, 7, 200)
      val specs1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        createPartitionSpecsBeforeCoalesce(1 to 6, bytesByPartitionId1) ++ // 1, 2, 3, 4, 5, 6
        Seq.fill(2)(CoalescedPartitionSpec(7, 8, 20)) // 7 - other side skew
      val specs2 =
        Seq.fill(3)(CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        createPartitionSpecsBeforeCoalesce(1 to 6, bytesByPartitionId2) ++ // 1, 2, 3, 4, 5, 6
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(7, i, i + 1, 100L)) // 7 - skew
      val expected1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        Seq(CoalescedPartitionSpec(1, 5, 33)) ++ // 1, 2, 3, 4 - coalesced
        Seq(CoalescedPartitionSpec(5, 7, 20)) ++ // 6, 7 - coalesced
        Seq.fill(2)(CoalescedPartitionSpec(7, 8, 20)) // 7 - other side skew
      val expected2 =
        Seq.fill(3)(CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        Seq(CoalescedPartitionSpec(1, 5, 38)) ++ // 1, 2, 3, 4 - coalesced
        Seq(CoalescedPartitionSpec(5, 7, 27)) ++ // 6, 7 - coalesced
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(7, i, i + 1, 100L)) // 7 - skew
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, 0)
      assert(coalesced == Seq(expected1, expected2))
    }

    {
      // Back-to-back skew sections.
      val bytesByPartitionId1 = Array[Long](300, 50, 10, 10, 8, 10, 400, 200)
      val bytesByPartitionId2 = Array[Long](10, 500, 10, 8, 10, 20, 7, 20)
      val specs1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        Seq.fill(5)(CoalescedPartitionSpec(1, 2, 50)) ++ // 1 - other side skew
        createPartitionSpecsBeforeCoalesce(2 to 5, bytesByPartitionId1) ++ // 2, 3, 4, 5
        Seq.tabulate(4)(i => PartialReducerPartitionSpec(6, i, i + 1, 100L)) ++ // 6 - skew
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(7, i, i + 1, 100L)) // 7 - skew
      val specs2 =
        Seq.fill(3)(CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        Seq.tabulate(5)(i => PartialReducerPartitionSpec(1, i, i + 1, 100L)) ++ // 1 - skew
        createPartitionSpecsBeforeCoalesce(2 to 5, bytesByPartitionId2) ++ // 2, 3, 4, 5
        Seq.fill(4)(CoalescedPartitionSpec(6, 7, 7)) ++ // 6 - other side skew
        Seq.fill(2)(CoalescedPartitionSpec(7, 8, 20)) // 7 - other side skew
      val expected1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        Seq.fill(5)(CoalescedPartitionSpec(1, 2, 50)) ++ // 1 - other side skew
        Seq(CoalescedPartitionSpec(2, 6, 38)) ++ // 2, 3, 4, 5 - coalesced
        Seq.tabulate(4)(i => PartialReducerPartitionSpec(6, i, i + 1, 100L)) ++ // 6 - skew
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(7, i, i + 1, 100L)) // 7 - skew
      val expected2 =
        Seq.fill(3)(CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        Seq.tabulate(5)(i => PartialReducerPartitionSpec(1, i, i + 1, 100L)) ++ // 1 - skew
        Seq(CoalescedPartitionSpec(2, 6, 48)) ++ // 2, 3, 4, 5 - coalesced
        Seq.fill(4)(CoalescedPartitionSpec(6, 7, 7)) ++ // 6 - other side skew
        Seq.fill(2)(CoalescedPartitionSpec(7, 8, 20)) // 7 - other side skew
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, 0)
      assert(coalesced == Seq(expected1, expected2))
    }

    {
      // Singled out partitions in between skew sections and at the end.
      val bytesByPartitionId1 = Array[Long](300, 16, 30, 10, 8, 10, 10, 20)
      val bytesByPartitionId2 = Array[Long](10, 10, 500, 8, 10, 7, 200, 20)
      val specs1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        Seq(CoalescedPartitionSpec(1, 2, 16)) ++ // 1
        Seq.fill(5)(CoalescedPartitionSpec(2, 3, 30)) ++ // 2 - other side skew
        createPartitionSpecsBeforeCoalesce(3 to 5, bytesByPartitionId1) ++ // 3, 4, 5
        Seq.fill(2)(CoalescedPartitionSpec(6, 7, 10)) ++ // 6 - other side skew
        Seq(CoalescedPartitionSpec(7, 8, 20)) // 7
      val specs2 =
        Seq.tabulate(3)(i => CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        Seq(CoalescedPartitionSpec(1, 2, 10)) ++ // 1
        Seq.tabulate(5)(i => PartialReducerPartitionSpec(2, i, i + 1, 100L)) ++ // 2- skew
        createPartitionSpecsBeforeCoalesce(3 to 5, bytesByPartitionId2) ++ // 3, 4, 5
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(6, i, i + 1, 100L)) ++ // 6 - skew
        Seq(CoalescedPartitionSpec(7, 8, 20)) // 7
      val expected1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        Seq(CoalescedPartitionSpec(1, 2, 16)) ++ // 1
        Seq.fill(5)(CoalescedPartitionSpec(2, 3, 30)) ++ // 2 - other side skew
        Seq(CoalescedPartitionSpec(3, 6, 28)) ++ // 3, 4, 5 - coalesced
        Seq.fill(2)(CoalescedPartitionSpec(6, 7, 10)) ++ // 6 - other side skew
        Seq(CoalescedPartitionSpec(7, 8, 20)) // 7
      val expected2 =
        Seq.tabulate(3)(i => CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        Seq(CoalescedPartitionSpec(1, 2, 10)) ++ // 1
        Seq.tabulate(5)(i => PartialReducerPartitionSpec(2, i, i + 1, 100L)) ++ // 2- skew
        Seq(CoalescedPartitionSpec(3, 6, 25)) ++ // 3, 4, 5 - coalesced
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(6, i, i + 1, 100L)) ++ // 6 - skew
        Seq(CoalescedPartitionSpec(7, 8, 20)) // 7
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, 0)
      assert(coalesced == Seq(expected1, expected2))
    }

    {
      // Partitions below `minPartitionSize` packed with smaller adjacent partition.
      val minPartitionSize = 10L
      val bytesByPartitionId1 = Array[Long](300, 90, 4, 16, 3, 24, 10, 60, 5)
      val bytesByPartitionId2 = Array[Long](10, 10, 5, 102, 4, 80, 200, 36, 3)
      val specs1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        createPartitionSpecsBeforeCoalesce(1 to 5, bytesByPartitionId1) ++ // 1, 2, 3, 4, 5
        Seq.fill(2)(CoalescedPartitionSpec(6, 7, 10)) ++ // 6 - other side skew
        createPartitionSpecsBeforeCoalesce(7 to 8, bytesByPartitionId1) // 7, 8
      val specs2 =
        Seq.tabulate(3)(i => CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        createPartitionSpecsBeforeCoalesce(1 to 5, bytesByPartitionId2) ++ // 1, 2, 3, 4, 5
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(6, i, i + 1, 100L)) ++ // 6 - skew
        createPartitionSpecsBeforeCoalesce(7 to 8, bytesByPartitionId2) // 7, 8
      val expected1 =
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(0, i, i + 1, 100L)) ++ // 0 - skew
        Seq(CoalescedPartitionSpec(1, 3, 94)) ++ // 1, 2 - coalesced
        Seq(CoalescedPartitionSpec(3, 4, 16)) ++ // 3
        Seq(CoalescedPartitionSpec(4, 6, 27)) ++ // 4, 5 - coalesced
        Seq.fill(2)(CoalescedPartitionSpec(6, 7, 10)) ++ // 6 - other side skew
        Seq(CoalescedPartitionSpec(7, 9, 65)) // 7, 8 - coalesced
      val expected2 =
        Seq.tabulate(3)(i => CoalescedPartitionSpec(0, 1, 10)) ++ // 0 - other side skew
        Seq(CoalescedPartitionSpec(1, 3, 15)) ++ // 1, 2 - coalesced
        Seq(CoalescedPartitionSpec(3, 4, 102)) ++ // 3
        Seq(CoalescedPartitionSpec(4, 6, 84)) ++ // 4, 5 - coalesced
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(6, i, i + 1, 100L)) ++ // 6 - skew
        Seq(CoalescedPartitionSpec(7, 9, 39)) // 7, 8 - coalesced
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, minPartitionSize)
      assert(coalesced == Seq(expected1, expected2))
    }

    {
      // No actual coalescing happened, return empty result.
      val bytesByPartitionId1 = Array[Long](5, 300, 40, 8, 20, 10, 3)
      val bytesByPartitionId2 = Array[Long](4, 10, 58, 10, 67, 200, 5)
      val specs1 =
        Seq(CoalescedPartitionSpec(0, 1, 5)) ++ // 0
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(1, i, i + 1, 100L)) ++ // 1 - skew
        createPartitionSpecsBeforeCoalesce(2 to 4, bytesByPartitionId1) ++ // 2, 3, 4
        Seq.fill(2)(CoalescedPartitionSpec(5, 6)) ++ // 5 - other side skew
        Seq(CoalescedPartitionSpec(6, 7)) // 6
      val specs2 =
        Seq(CoalescedPartitionSpec(0, 1)) ++ // 0
        Seq.fill(3)(CoalescedPartitionSpec(1, 2)) ++ // 1 - other side skew
        createPartitionSpecsBeforeCoalesce(2 to 4, bytesByPartitionId2) ++ // 2, 3, 4
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(5, i, i + 1, 100L)) ++ // 5 - skew
        Seq(CoalescedPartitionSpec(6, 7)) // 6
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, 0)
      assert(coalesced.isEmpty)
    }
  }

  test("coalesce after skew splitting - counter cases") {
    val targetSize = 100

    {
      // If not all shuffles have shuffle partition specs, return empty result.
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq.tabulate(5)(i => CoalescedPartitionSpec(i, i + 1))
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          Some(new MapOutputStatistics(1, bytesByPartitionId2))),
        Seq(
          Some(specs1),
          None),
        targetSize, 1, 0)
      assert(coalesced.isEmpty)
    }

    {
      // If not all shuffles have map output stats, return empty result.
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq.tabulate(5)(i => CoalescedPartitionSpec(i, i + 1))
      val specs2 = specs1
      val coalesced = ShufflePartitionsUtil.coalescePartitions(
        Array(
          Some(new MapOutputStatistics(0, bytesByPartitionId1)),
          None),
        Seq(
          Some(specs1),
          Some(specs2)),
        targetSize, 1, 0)
      assert(coalesced.isEmpty)
    }

    {
      // Assertion error if shuffle partition specs contain `CoalescedShuffleSpec` that has
      // `end` - `start` > 1.
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq(CoalescedPartitionSpec(0, 1), CoalescedPartitionSpec(1, 5))
      val specs2 = specs1
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }

    {
      // Assertion error if shuffle partition specs contain `PartialMapperShuffleSpec`.
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq(CoalescedPartitionSpec(0, 1), PartialMapperPartitionSpec(1, 0, 1))
      val specs2 = specs1
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }

    {
      // Assertion error if partition specs of different shuffles have different lengths.
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq.tabulate(4)(i => CoalescedPartitionSpec(i, i + 1)) ++
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(4, i, i + 1, 10L))
      val specs2 = Seq.tabulate(5)(i => CoalescedPartitionSpec(i, i + 1))
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }

    {
      // Assertion error if start indices of partition specs are not identical among all shuffles.
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq.tabulate(4)(i => CoalescedPartitionSpec(i, i + 1)) ++
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(4, i, i + 1, 10L))
      val specs2 = Seq.tabulate(2)(i => CoalescedPartitionSpec(i, i + 1)) ++
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(2, i, i + 1, 10L)) ++
        Seq.tabulate(2)(i => CoalescedPartitionSpec(i + 3, i + 4))
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }

    {
      // Assertion error if partition indices are out of order (in the beginning).
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq.tabulate(4)(i => CoalescedPartitionSpec(i + 1, i + 2))
      val specs2 = specs1
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }

    {
      // Assertion error if partition indices are out of order (before skew section).
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = (Seq.tabulate(2)(i => CoalescedPartitionSpec(i, i + 1)) :+
        CoalescedPartitionSpec(0, 1)) ++ Seq.fill(3)(CoalescedPartitionSpec(2, 3))
      val specs2 = specs1
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }

    {
      // Assertion error if partition indices are out of order (after skew section).
      val bytesByPartitionId1 = Array[Long](10, 10, 10, 10, 10)
      val bytesByPartitionId2 = Array[Long](10, 10, 10, 10, 10)
      val specs1 = Seq.fill(2)(CoalescedPartitionSpec(0, 1)) ++
        Seq.tabulate(2)(i => CoalescedPartitionSpec(i + 2, i + 3)) :+ CoalescedPartitionSpec(4, 5)
      val specs2 = specs1
      intercept[AssertionError] {
        ShufflePartitionsUtil.coalescePartitions(
          Array(
            Some(new MapOutputStatistics(0, bytesByPartitionId1)),
            Some(new MapOutputStatistics(1, bytesByPartitionId2))),
          Seq(
            Some(specs1),
            Some(specs2)),
          targetSize, 1, 0)
      }
    }
  }

  test("splitSizeListByTargetSize") {
    val targetSize = 100

    val smallPartitionFactor1 = ShufflePartitionsUtil.SMALL_PARTITION_FACTOR
    // merge the small partitions at the beginning/end
    val sizeList1 = Seq[Long](15, 90, 15, 15, 15, 90, 15)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList1, targetSize, smallPartitionFactor1).toSeq ==
      Seq(0, 2, 5))

    // merge the small partitions in the middle
    val sizeList2 = Seq[Long](30, 15, 90, 10, 90, 15, 30)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList2, targetSize, smallPartitionFactor1).toSeq ==
      Seq(0, 2, 4, 5))

    // merge small partitions if the partition itself is smaller than
    // targetSize * SMALL_PARTITION_FACTOR
    val sizeList3 = Seq[Long](15, 1000, 15, 1000)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList3, targetSize, smallPartitionFactor1).toSeq ==
      Seq(0, 3))

    // merge small partitions if the combined size is smaller than
    // targetSize * MERGED_PARTITION_FACTOR
    val sizeList4 = Seq[Long](35, 75, 90, 20, 35, 25, 35)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList4, targetSize, smallPartitionFactor1).toSeq ==
      Seq(0, 2, 3))

    val smallPartitionFactor2 = 0.5
    // merge last two partition if their size is not bigger than smallPartitionFactor * target
    val sizeList5 = Seq[Long](50, 50, 40, 5)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList5, targetSize, smallPartitionFactor2).toSeq ==
      Seq(0))

    val sizeList6 = Seq[Long](40, 5, 50, 45)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList6, targetSize, smallPartitionFactor2).toSeq ==
      Seq(0))

    // do not merge
    val sizeList7 = Seq[Long](50, 50, 10, 40, 5)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList7, targetSize, smallPartitionFactor2).toSeq ==
      Seq(0, 2))

    val sizeList8 = Seq[Long](10, 40, 5, 50, 50)
    assert(ShufflePartitionsUtil.splitSizeListByTargetSize(
      sizeList8, targetSize, smallPartitionFactor2).toSeq ==
      Seq(0, 3))
  }

  test("SPARK-35923: Coalesce empty partition with mixed CoalescedPartitionSpec and" +
    "PartialReducerPartitionSpec") {
    val targetSize = 100
    val bytesByPartitionId1 = Array[Long](0, 200, 0, 10, 0, 0, 400, 0, 0)
    val bytesByPartitionId2 = Array[Long](0, 30, 10, 300, 0, 0, 5, 0, 0)
    val specs1 =
      Seq(CoalescedPartitionSpec(0, 1, 0)) ++ // 0 - empty
        Seq.tabulate(2)(i => PartialReducerPartitionSpec(1, i, i + 1, 100)) ++ // 1 - skew
        Seq(CoalescedPartitionSpec(2, 3, 0)) ++ // 2
        Seq.fill(3)(CoalescedPartitionSpec(3, 4, 10)) ++ // 3 - other side skew
        Seq.tabulate(2)(i => CoalescedPartitionSpec(4 + i, 5 + i, 0)) ++ // 4, 5 - empty
        Seq.tabulate(4)(i => PartialReducerPartitionSpec(6, i, i + 1, 100)) ++ // 6 - skew
        Seq.tabulate(2)(i => CoalescedPartitionSpec(7 + i, 8 + i, 0)) // 7, 8 - empty
    val specs2 =
      Seq(CoalescedPartitionSpec(0, 1, 0)) ++ // 0 - empty
        Seq.fill(2)(CoalescedPartitionSpec(1, 2, 30)) ++ // 1 - other side skew
        Seq(CoalescedPartitionSpec(2, 3, 10)) ++ // 2
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(3, i, i + 1, 100)) ++ // 3 - skew
        Seq.tabulate(2)(i => CoalescedPartitionSpec(4 + i, 5 + i, 0)) ++ // 4, 5 - empty
        Seq.fill(4)(CoalescedPartitionSpec(6, 7, 5)) ++ // 6 - other side skew
        Seq.tabulate(2)(i => CoalescedPartitionSpec(7 + i, 8 + i, 0)) // 7, 8 - empty
    val expected1 =
      Seq.tabulate(2)(i => PartialReducerPartitionSpec(1, i, i + 1, 100)) ++ // 1 - skew
        Seq(CoalescedPartitionSpec(2, 3, 0)) ++ // 2 not coalesce since other shuffle is not empty
        Seq.fill(3)(CoalescedPartitionSpec(3, 4, 10)) ++ // 3 - other side skew
        Seq.tabulate(4)(i => PartialReducerPartitionSpec(6, i, i + 1, 100)) // 6 - skew
    val expected2 =
      Seq.fill(2)(CoalescedPartitionSpec(1, 2, 30)) ++ // 1 - other side skew
        Seq(CoalescedPartitionSpec(2, 3, 10)) ++ // 2
        Seq.tabulate(3)(i => PartialReducerPartitionSpec(3, i, i + 1, 100)) ++ // 3 - skew
        Seq.fill(4)(CoalescedPartitionSpec(6, 7, 5)) // 6 - other side skew
    val coalesced = ShufflePartitionsUtil.coalescePartitions(
      Array(
        Some(new MapOutputStatistics(0, bytesByPartitionId1)),
        Some(new MapOutputStatistics(1, bytesByPartitionId2))),
      Seq(
        Some(specs1),
        Some(specs2)),
      targetSize, 1, 0)
    assert(coalesced == Seq(expected1, expected2))
  }

  test("SPARK-36228: Skip splitting a skewed partition when some map outputs are removed") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[2]"))
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.registerShuffle(shuffleId = 10, numMaps = 2, numReduces = 1)
    mapOutputTracker.registerMapOutput(shuffleId = 10, mapIndex = 0, MapStatus(
      BlockManagerId("a", "hostA", port = 1000),
      Array(MapStatus.compressSize(10)),
      mapTaskId = 5))
    mapOutputTracker.registerMapOutput(shuffleId = 10, mapIndex = 1, MapStatus(
      BlockManagerId("b", "hostB", port = 1000),
      Array(MapStatus.compressSize(20)),
      mapTaskId = 6))

    val skewPartitionSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
      shuffleId = 10, reducerId = 0, targetSize = 2)
    assert(skewPartitionSpecs.isDefined)
    // Returns 2 partition specs because there are 2 mappers.
    assert(skewPartitionSpecs.get.size == 2)

    // As if one map output is removed
    mapOutputTracker.unregisterMapOutput(
      shuffleId = 10, mapIndex = 0, BlockManagerId("a", "hostA", port = 1000))
    val skewPartitionSpecsAfterRemoval = ShufflePartitionsUtil.createSkewPartitionSpecs(
      shuffleId = 10, reducerId = 0, targetSize = 2)
    assert(skewPartitionSpecsAfterRemoval.isEmpty)
  }
}
