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

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{MapOutputStatistics, SparkConf, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.exchange.{ExchangeCoordinator, ShuffleExchange}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class ExchangeCoordinatorSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    // Set these states back.
    originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
    originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
  }

  private def checkEstimation(
      coordinator: ExchangeCoordinator,
      bytesByPartitionIdArray: Array[Array[Long]],
      expectedPartitionStartIndices: Array[Int]): Unit = {
    val mapOutputStatistics = bytesByPartitionIdArray.zipWithIndex.map {
      case (bytesByPartitionId, index) =>
        new MapOutputStatistics(index, bytesByPartitionId)
    }
    val estimatedPartitionStartIndices =
      coordinator.estimatePartitionStartIndices(mapOutputStatistics)
    assert(estimatedPartitionStartIndices === expectedPartitionStartIndices)
  }

  private def checkSkewPartition(
      coordinator: ExchangeCoordinator,
      bytesByPartitionIdArray: Array[Array[Long]],
      preStageNum: Array[Int],
      expectedSkewPartitionIndices: Array[(Int, Array[(Int, Long, Int, Int)])]): Unit = {
    val mapOutputStatistics = bytesByPartitionIdArray.zipWithIndex.map {
      case (bytesByPartitionId, index) =>
        new MapOutputStatistics(index, bytesByPartitionId)
    }
    val estimatedPartitionStartIndices =
      coordinator.estimatePartitionStartIndices(mapOutputStatistics)
    val skewPartitionIndices =
      coordinator.skewPartitionIdx(mapOutputStatistics,
        preStageNum,
        Some(estimatedPartitionStartIndices))
    assert(skewPartitionIndices.length === 2)
    assert(skewPartitionIndices(0)._1 === expectedSkewPartitionIndices(0)._1)
    assert(skewPartitionIndices(1)._1 === expectedSkewPartitionIndices(1)._1)
    assert(skewPartitionIndices(0)._2 === expectedSkewPartitionIndices(0)._2)
    assert(skewPartitionIndices(1)._2 === expectedSkewPartitionIndices(1)._2)
  }

  test("test skewPartitionIdx ") {
    val coordinator = new ExchangeCoordinator(2, 100L, None, 500L, true, Inner)

    {
      // All bytes per partition are 0.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (0, Array((-1, 0L, 0, 0))),
          (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }

    {
      // one partition skew
      val bytesByPartitionId1 = Array[Long](10, 5009, 10, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 4, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (4, Array(
            (-1, 0L, 0, 1),
            (1, 5009L, 1, 2),
            (-1, 0L, 2, 1)
          )),
          (4, Array(
            (-1, 0L, 0, 1),
            (2, 5009L, 1, 2),
            (-1, 0L, 2, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
    {
      // two partition skew
      val bytesByPartitionId1 = Array[Long](10, 5009, 600, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 4, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (6, Array(
            (-1, 0L, 0, 1),
            (1, 5009L, 1, 2),
            (1, 600L, 2, 2),
            (-1, 0L, 3, 1)
          )),
          (6, Array(
            (-1, 0L, 0, 1),
            (2, 5009L, 1, 2),
            (2, 600L, 2, 2),
            (-1, 0L, 3, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }

    {
      // 2 side partition is more than skewthreadhold
      val bytesByPartitionId1 = Array[Long](10, 5009, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 4000, 3, 4, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (4, Array(
            (-1, 0L, 0, 1),
            (1, 5009L, 1, 2),
            (-1, 0L, 2, 1)
          )),
          (4, Array(
            (-1, 0L, 0, 1),
            (2, 5009L, 1, 2),
            (-1, 0L, 2, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
    {
      // 2 side partition have skew partition
      val bytesByPartitionId1 = Array[Long](10, 5009, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 5009, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (8, Array(
            (-1, 0L, 0, 1),
            (1, 5009L, 1, 2),
            (-1, 0L, 2, 1),
            (2, 5009L, 3, 3),
            (-1, 0L, 4, 1)
          )),
          (8, Array(
            (-1, 0L, 0, 1),
            (2, 5009L, 1, 2),
            (-1, 0L, 2, 1),
            (1, 5009L, 3, 3),
            (-1, 0L, 4, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
    {
      // first partition have skew partition
      val bytesByPartitionId1 = Array[Long](5009, 1, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 4, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (3, Array(
            (1, 5009L, 0, 2),
            (-1, 0L, 1, 1)
          )),
          (3, Array(
            (2, 5009L, 0, 2),
            (-1, 0L, 1, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
    {
      val bytesByPartitionId1 = Array[Long](5009, 1, 5009, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 4, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (6, Array(
            (1, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (1, 5009L, 2, 2),
            (-1, 0L, 3, 1)
          )),
          (6, Array(
            (2, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (2, 5009L, 2, 2),
            (-1, 0L, 3, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
    {
      val bytesByPartitionId1 = Array[Long](5009, 1, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 4, 5009)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (6, Array(
            (1, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (2, 5009L, 4, 3)
          )),
          (6, Array(
            (2, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (1, 5009L, 4, 3)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
  }

  test("test skewPartitionIdx when need read a range of maps") {
    val coordinator = new ExchangeCoordinator(2, 100L, None, 500L, true, Inner)

    {
      val bytesByPartitionId1 = Array[Long](1, 501, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 5, 2, 1, 1)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (4, Array(
            (-1, 0L, 0, 1),
            (1, 501L, 1, 4),
            (-1, 0L, 2, 1)
          )),
          (4, Array(
            (-1, 0L, 0, 1),
            (2, 501L, 1, 4),
            (-1, 0L, 2, 1)
          ))
        )
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](4, 3),
        expectedPartitionStartIndices)
    }

    {
      val bytesByPartitionId1 = Array[Long](1, 5001, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 5, 2, 1, 1)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (13, Array(
            (-1, 0L, 0, 1),
            (1, 5001L, 1, 40),
            (-1, 0L, 2, 1)
          )),
          (13, Array(
            (-1, 0L, 0, 1),
            (2, 5001L, 1, 40),
            (-1, 0L, 2, 1)
          ))
        )
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](40, 3),
        expectedPartitionStartIndices)
    }

    {
      val bytesByPartitionId1 = Array[Long](1, 5001, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 5, 2000, 1, 1)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (18, Array(
            (-1, 0L, 0, 1),
            (1, 5001L, 1, 40),
            (2, 2000L, 2, 30),
            (-1, 0L, 3, 1)
          )),
          (18, Array(
            (-1, 0L, 0, 1),
            (2, 5001L, 1, 40),
            (1, 2000L, 2, 30),
            (-1, 0L, 3, 1)
          ))
        )
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](40, 30),
        expectedPartitionStartIndices)
    }

    {
      val bytesByPartitionId1 = Array[Long](1, 5001, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](2000, 5, 2000, 1, 1)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (22, Array(
            (2, 2000L, 0, 30),
            (1, 5001L, 1, 40),
            (2, 2000L, 2, 30),
            (-1, 0L, 3, 1)
          )),
          (22, Array(
            (1, 2000L, 0, 30),
            (2, 5001L, 1, 40),
            (1, 2000L, 2, 30),
            (-1, 0L, 3, 1)
          ))
        )
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](40, 30),
        expectedPartitionStartIndices)
    }
  }

  test ("test skewPartitionIdx when left out join") {
    val coordinator = new ExchangeCoordinator(2, 100L, None, 500L, true, LeftOuter)

    {
      val bytesByPartitionId1 = Array[Long](10, 20, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 5009, 5)
      val expectedPartitionStartIndices =
      Array[(Int, Array[(Int, Long, Int, Int)])](
        (0, Array((-1, 0L, 0, 0))),
        (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }

    {
      val bytesByPartitionId1 = Array[Long](10, 5001, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 5009, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (0, Array((-1, 0L, 0, 0))),
          (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }
    {
      val bytesByPartitionId1 = Array[Long](5009, 1, 5009, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 4, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (6, Array(
            (1, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (1, 5009L, 2, 2),
            (-1, 0L, 3, 1)
          )),
          (6, Array(
            (2, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (2, 5009L, 2, 2),
            (-1, 0L, 3, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](2, 3),
        expectedPartitionStartIndices)
    }
  }

  test ("test skewPartitionIdx when right out join") {
    val coordinator = new ExchangeCoordinator(2, 100L, None, 500L, true, RightOuter)

    {
      val bytesByPartitionId1 = Array[Long](10, 5001, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 5009, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (0, Array((-1, 0L, 0, 0))),
          (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }

    {
      val bytesByPartitionId1 = Array[Long](10, 20, 2, 5009, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 11, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (0, Array((-1, 0L, 0, 0))),
          (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }
    {
      val bytesByPartitionId1 = Array[Long](1, 2, 3, 4, 5)
      val bytesByPartitionId2 = Array[Long](5009, 1, 5009, 1, 1)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (6, Array(
            (2, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (2, 5009L, 2, 2),
            (-1, 0L, 3, 1)
          )),
          (6, Array(
            (1, 5009L, 0, 2),
            (-1, 0L, 1, 1),
            (1, 5009L, 2, 2),
            (-1, 0L, 3, 1)
          )))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](3, 2),
        expectedPartitionStartIndices)
    }
  }
  test ("test skewPartitionIdx when full out join") {
    val coordinator = new ExchangeCoordinator(2, 100L, None, 500L, true, FullOuter)

    {
      val bytesByPartitionId1 = Array[Long](10, 5001, 2, 1, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 5009, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](

          (0, Array((-1, 0L, 0, 0))),
          (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }

    {
      val bytesByPartitionId1 = Array[Long](10, 20, 2, 5009, 1)
      val bytesByPartitionId2 = Array[Long](1, 2, 3, 11, 5)
      val expectedPartitionStartIndices =
        Array[(Int, Array[(Int, Long, Int, Int)])](
          (0, Array((-1, 0L, 0, 0))),
          (0, Array((-1, 0L, 0, 0))))
      checkSkewPartition(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        Array[Int](5, 5),
        expectedPartitionStartIndices)
    }
  }

  test("test estimatePartitionStartIndices - 1 Exchange") {
    val coordinator = new ExchangeCoordinator(1, 100L)

    {
      // All bytes per partition are 0.
      val bytesByPartitionId = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // Some bytes per partition are 0 and total size is less than the target size.
      // 1 post-shuffle partition is needed.
      val bytesByPartitionId = Array[Long](10, 0, 20, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partitions are needed.
      val bytesByPartitionId = Array[Long](10, 0, 90, 20, 0)
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // There are a few large pre-shuffle partitions.
      val bytesByPartitionId = Array[Long](110, 10, 100, 110, 0)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // All pre-shuffle partitions are larger than the targeted size.
      val bytesByPartitionId = Array[Long](100, 110, 100, 110, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // The last pre-shuffle partition is in a single post-shuffle partition.
      val bytesByPartitionId = Array[Long](30, 30, 0, 40, 110)
      val expectedPartitionStartIndices = Array[Int](0, 4)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }
  }


  test("test estimatePartitionStartIndices - 2 Exchanges") {
    val coordinator = new ExchangeCoordinator(2, 100L)

    {
      // If there are multiple values of the number of pre-shuffle partitions,
      // we should see an assertion error.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0, 0)
      val mapOutputStatistics =
        Array(
          new MapOutputStatistics(0, bytesByPartitionId1),
          new MapOutputStatistics(1, bytesByPartitionId2))
      intercept[AssertionError](coordinator.estimatePartitionStartIndices(mapOutputStatistics))
    }

    {
      // All bytes per partition are 0.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // Some bytes per partition are 0.
      // 1 post-shuffle partition is needed.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 20, 0, 20)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 4 post-shuffle partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 99, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 100, 0, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // There are a few large pre-shuffle partitions.
      val bytesByPartitionId1 = Array[Long](0, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 0, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // All pairs of pre-shuffle partitions are larger than the targeted size.
      val bytesByPartitionId1 = Array[Long](100, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 70, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }
  }

  test("test estimatePartitionStartIndices and enforce minimal number of reducers") {
    val coordinator = new ExchangeCoordinator(2, 100L, Some(2))

    {
      // The minimal number of post-shuffle partitions is not enforced because
      // the size of data is 0.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // The minimal number of post-shuffle partitions is enforced.
      val bytesByPartitionId1 = Array[Long](10, 5, 5, 0, 20)
      val bytesByPartitionId2 = Array[Long](5, 10, 0, 10, 5)
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // The number of post-shuffle partitions is determined by the coordinator.
      val bytesByPartitionId1 = Array[Long](10, 50, 20, 80, 20)
      val bytesByPartitionId2 = Array[Long](40, 10, 0, 10, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 3, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Query tests
  ///////////////////////////////////////////////////////////////////////////

  val numInputPartitions: Int = 10

  def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(actual, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  def withSparkSession(
      f: SparkSession => Unit,
      targetNumPostShufflePartitions: Int,
      minNumPostShufflePartitions: Option[Int]): Unit = {
    val sparkConf =
      new SparkConf(false)
        .setMaster("local[*]")
        .setAppName("test")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.allowMultipleContexts", "true")
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(
          SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key,
          targetNumPostShufflePartitions.toString)
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS.key, "-1")
    }

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark) finally spark.stop()
  }

  Seq(Some(5), None).foreach { minNumPostShufflePartitions =>
    val testNameNote = minNumPostShufflePartitions match {
      case Some(numPartitions) => "(minNumPostShufflePartitions: 3)"
      case None => ""
    }

    test(s"determining the number of reducers: aggregate operator$testNameNote") {
      val test = { spark: SparkSession =>
        val df =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 20 as key", "id as value")
        val agg = df.groupBy("key").count()

        // Check the answer first.
        checkAnswer(
          agg,
          spark.range(0, 20).selectExpr("id", "50 as cnt").collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = agg.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 1)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 3)
              case o =>
            }
        }
      }

      withSparkSession(test, 2000, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: join operator$testNameNote") {
      val test = { spark: SparkSession =>
        val df1 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
        val df2 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")

        val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

        // Check the answer first.
        val expectedAnswer =
          spark
            .range(0, 1000)
            .selectExpr("id % 500 as key", "id as value")
            .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 2)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 2)
              case o =>
            }
        }
      }

      withSparkSession(test, 16384, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 1$testNameNote") {
      val test = { spark: SparkSession =>
        val df1 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
            .groupBy("key1")
            .count()
            .toDF("key1", "cnt1")
        val df2 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")
            .groupBy("key2")
            .count()
            .toDF("key2", "cnt2")

        val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("cnt2"))

        // Check the answer first.
        val expectedAnswer =
          spark
            .range(0, 500)
            .selectExpr("id", "2 as cnt")
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 4)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            assert(exchanges.forall(_.coordinator.isDefined))
            assert(exchanges.map(_.outputPartitioning.numPartitions).toSet === Set(2, 3))
        }
      }

      withSparkSession(test, 6644, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 2$testNameNote") {
      val test = { spark: SparkSession =>
        val df1 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
            .groupBy("key1")
            .count()
            .toDF("key1", "cnt1")
        val df2 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")

        val join =
          df1
            .join(df2, col("key1") === col("key2"))
            .select(col("key1"), col("cnt1"), col("value2"))

        // Check the answer first.
        val expectedAnswer =
          spark
            .range(0, 1000)
            .selectExpr("id % 500 as key", "2 as cnt", "id as value")
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 3)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            assert(exchanges.forall(_.coordinator.isDefined))
            assert(exchanges.map(_.outputPartitioning.numPartitions).toSet === Set(5, 3))
        }
      }

      withSparkSession(test, 6144, minNumPostShufflePartitions)
    }
  }
  // skew data run test
  test(s"test skew join") {
    val test = { spark: SparkSession =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEW_JOIN.key, 300)
      import spark.implicits._
       val df1 = spark.sparkContext.makeRDD(1 to 10).
         map(i => {
           ((if (i < 5) 9999 else if (i == 8) 8888 else 500 + i), "b" + i, "skew test")
         }).
         toDF("sid", "sname", "scomment")
      val df2 = spark.sparkContext.makeRDD(1 to 10).map(i => {
        ((if (i == 5) 9999 else if (i < 5) 8888 else i), "a" + i, "test skew")
      }).toDF("rid", "rname", "rcomment")
      val join = df1.join(df2, $"sid"===$"rid")
      assert(join.collect().length === 8)
    }
    withSparkSession(test, 30, Some(3))
  }
  test(s"test skew join 2") {
    val test = { spark: SparkSession =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEW_JOIN.key, 500)
      import spark.implicits._
      val df1 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 100) 9999 else 500 + i), "b" + i, "skew test")
      }).toDF("sid", "sname", "scomment")
      val df2 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 50) 9999 else if (i < 500) 555 else i), "a" + i, "test skew")
      }).toDF("rid", "rname", "rcomment")
      val join = df1.join(df2, $"sid"===$"rid")
      assert(join.collect().length === 5252)
    }
    withSparkSession(test, 300, Some(3))
  }

  test(s"test skew join 3") {
    val test = { spark: SparkSession =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEW_JOIN.key, 1000)
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 50)
      import spark.implicits._
      val df1 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 100) 9999 else 500 + i), "b" + i, "skew test")
      }).toDF("sid", "sname", "scomment")
      val df2 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 50) 9999 else if (i < 500) 555 else i), "a" + i, "test skew")
      }).toDF("rid", "rname", "rcomment")
      val join = df1.join(df2, $"sid"===$"rid")
      assert(join.collect().length === 5252)
    }
    withSparkSession(test, 300, Some(3))
  }

  test(s"test skew join 4") {
    val test = { spark: SparkSession =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEW_JOIN.key, 900)
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 5)
      import spark.implicits._
      val df1 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 100) 9999 else 500 + i), "b" + i, "skew test")
      }).toDF("sid", "sname", "scomment")
      val df2 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 50) 9999 else if (i < 500) 555 else i), "a" + i, "test skew")
      }).toDF("rid", "rname", "rcomment")
      val join = df1.join(df2, $"sid"===$"rid")
      assert(join.collect().length === 5252)
    }
    withSparkSession(test, 300, Some(3))
  }

  test(s"test skew out join ") {
    val test = { spark: SparkSession =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_SKEW_JOIN.key, 900)
      spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 5)
      import spark.implicits._
      val df1 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 30) 9999 else 500 + i), "b" + i, "skew test")
      }).toDF("sid", "sname", "scomment")
      val df2 = spark.sparkContext.makeRDD(1 to 1000).map(i => {
        ((if (i < 50) 9999 else if (i < 500) 555 else i), "a" + i, "test skew")
      }).toDF("rid", "rname", "rcomment")
      val leftjoin = df1.join(df2, $"sid"===$"rid", "leftouter")
      assert(leftjoin.collect().length === 2842)
      val rightjoin = df1.join(df2, $"sid"===$"rid", "rightouter")
      assert(rightjoin.collect().length === 2372)
      val fulljoin = df1.join(df2, $"sid"===$"rid", "fullouter")
      assert(fulljoin.collect().length === 2872)
    }
    withSparkSession(test, 300, Some(3))
  }
}
