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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql._
import org.apache.spark.{SparkFunSuite, SparkContext, SparkConf, MapOutputStatistics}

class ExchangeCoordinatorSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSQLContext: Option[SQLContext] = _
  private var originalInstantiatedSQLContext: Option[SQLContext] = _

  override protected def beforeAll(): Unit = {
    originalActiveSQLContext = SQLContext.getActive()
    originalInstantiatedSQLContext = SQLContext.getInstantiatedContextOption()

    SQLContext.clearActive()
    SQLContext.clearInstantiatedContext()
  }

  override protected def afterAll(): Unit = {
    // Set these states back.
    originalActiveSQLContext.foreach(ctx => SQLContext.setActive(ctx))
    originalInstantiatedSQLContext.foreach(ctx => SQLContext.setInstantiatedContext(ctx))
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
      val expectedPartitionStartIndices = Array[Int](0, 1, 3, 4)
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
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 99, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 2)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 100, 0, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // There are a few large pre-shuffle partitions.
      val bytesByPartitionId1 = Array[Long](0, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 0, 110)
      val expectedPartitionStartIndices = Array[Int](0, 2, 3)
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
      val expectedPartitionStartIndices = Array[Int](0, 2, 4)
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

  def withSQLContext(
      f: SQLContext => Unit,
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
        .set(
          SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key,
          targetNumPostShufflePartitions.toString)
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS.key, "-1")
    }
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new TestSQLContext(sparkContext)
    try f(sqlContext) finally sparkContext.stop()
  }

  Seq(Some(3), None).foreach { minNumPostShufflePartitions =>
    val testNameNote = minNumPostShufflePartitions match {
      case Some(numPartitions) => "(minNumPostShufflePartitions: 3)"
      case None => ""
    }

    test(s"determining the number of reducers: aggregate operator$testNameNote") {
      val test = { sqlContext: SQLContext =>
        val df =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 20 as key", "id as value")
        val agg = df.groupBy("key").count

        // Check the answer first.
        checkAnswer(
          agg,
          sqlContext.range(0, 20).selectExpr("id", "50 as cnt").collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = agg.queryExecution.executedPlan.collect {
          case e: Exchange => e
        }
        assert(exchanges.length === 1)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: Exchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 3)
              case o =>
            }

          case None =>
            exchanges.foreach {
              case e: Exchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 2)
              case o =>
            }
        }
      }

      withSQLContext(test, 1700, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: join operator$testNameNote") {
      val test = { sqlContext: SQLContext =>
        val df1 =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
        val df2 =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")

        val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

        // Check the answer first.
        val expectedAnswer =
          sqlContext
            .range(0, 1000)
            .selectExpr("id % 500 as key", "id as value")
            .unionAll(sqlContext.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: Exchange => e
        }
        assert(exchanges.length === 2)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: Exchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 3)
              case o =>
            }

          case None =>
            exchanges.foreach {
              case e: Exchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 2)
              case o =>
            }
        }
      }

      withSQLContext(test, 16384, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 1$testNameNote") {
      val test = { sqlContext: SQLContext =>
        val df1 =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
            .groupBy("key1")
            .count
            .toDF("key1", "cnt1")
        val df2 =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")
            .groupBy("key2")
            .count
            .toDF("key2", "cnt2")

        val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("cnt2"))

        // Check the answer first.
        val expectedAnswer =
          sqlContext
            .range(0, 500)
            .selectExpr("id", "2 as cnt")
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: Exchange => e
        }
        assert(exchanges.length === 4)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: Exchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 3)
              case o =>
            }

          case None =>
            assert(exchanges.forall(_.coordinator.isDefined))
            assert(exchanges.map(_.outputPartitioning.numPartitions).toSeq.toSet === Set(1, 2))
        }
      }

      withSQLContext(test, 6144, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 2$testNameNote") {
      val test = { sqlContext: SQLContext =>
        val df1 =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
            .groupBy("key1")
            .count
            .toDF("key1", "cnt1")
        val df2 =
          sqlContext
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")

        val join =
          df1
            .join(df2, col("key1") === col("key2"))
            .select(col("key1"), col("cnt1"), col("value2"))

        // Check the answer first.
        val expectedAnswer =
          sqlContext
            .range(0, 1000)
            .selectExpr("id % 500 as key", "2 as cnt", "id as value")
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: Exchange => e
        }
        assert(exchanges.length === 3)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: Exchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 3)
              case o =>
            }

          case None =>
            assert(exchanges.forall(_.coordinator.isDefined))
            assert(exchanges.map(_.outputPartitioning.numPartitions).toSeq.toSet === Set(2, 3))
        }
      }

      withSQLContext(test, 6144, minNumPostShufflePartitions)
    }
  }
}
