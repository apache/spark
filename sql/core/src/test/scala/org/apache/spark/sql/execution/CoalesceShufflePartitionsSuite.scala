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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class CoalesceShufflePartitionsSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    try {
      // Set these states back.
      originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
      originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
    } finally {
      super.afterAll()
    }
  }

  val numInputPartitions: Int = 10

  def withSparkSession(
      f: SparkSession => Unit,
      targetPostShuffleInputSize: Int,
      minNumPostShufflePartitions: Option[Int],
      enableIOEncryption: Boolean = false): Unit = {
    val sparkConf =
      new SparkConf(false)
        .setMaster("local[*]")
        .setAppName("test")
        .set(UI_ENABLED, false)
        .set(IO_ENCRYPTION_ENABLED, enableIOEncryption)
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key,
          targetPostShuffleInputSize.toString)
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
    }

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark) finally spark.stop()
  }

  Seq(Some(5), None).foreach { minNumPostShufflePartitions =>
    val testNameNote = minNumPostShufflePartitions match {
      case Some(numPartitions) => "(minNumPostShufflePartitions: " + numPartitions + ")"
      case None => ""
    }

    test(s"determining the number of reducers: aggregate operator$testNameNote") {
      val test: SparkSession => Unit = { spark: SparkSession =>
        val df =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 20 as key", "id as value")
        val agg = df.groupBy("key").count()

        // Check the answer first.
        QueryTest.checkAnswer(
          agg,
          spark.range(0, 20).selectExpr("id", "50 as cnt").collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val finalPlan = agg.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
        val shuffleReaders = finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }

        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            assert(shuffleReaders.isEmpty)

          case None =>
            assert(shuffleReaders.length === 1)
            shuffleReaders.foreach { reader =>
              assert(reader.outputPartitioning.numPartitions === 3)
            }
        }
      }

      withSparkSession(test, 2000, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: join operator$testNameNote") {
      val test: SparkSession => Unit = { spark: SparkSession =>
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
        QueryTest.checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val finalPlan = join.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
        val shuffleReaders = finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }

        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            assert(shuffleReaders.isEmpty)

          case None =>
            assert(shuffleReaders.length === 2)
            shuffleReaders.foreach { reader =>
              assert(reader.outputPartitioning.numPartitions === 2)
            }
        }
      }

      withSparkSession(test, 16384, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 1$testNameNote") {
      val test: (SparkSession) => Unit = { spark: SparkSession =>
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
        QueryTest.checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val finalPlan = join.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
        val shuffleReaders = finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }

        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            assert(shuffleReaders.isEmpty)

          case None =>
            assert(shuffleReaders.length === 2)
            shuffleReaders.foreach { reader =>
              assert(reader.outputPartitioning.numPartitions === 2)
            }
        }
      }

      withSparkSession(test, 16384, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 2$testNameNote") {
      val test: (SparkSession) => Unit = { spark: SparkSession =>
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
        QueryTest.checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val finalPlan = join.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
        val shuffleReaders = finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }

        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            assert(shuffleReaders.isEmpty)

          case None =>
            assert(shuffleReaders.length === 2)
            shuffleReaders.foreach { reader =>
              assert(reader.outputPartitioning.numPartitions === 3)
            }
        }
      }

      withSparkSession(test, 12000, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: plan already partitioned$testNameNote") {
      val test: SparkSession => Unit = { spark: SparkSession =>
        try {
          spark.range(1000).write.bucketBy(30, "id").saveAsTable("t")
          // `df1` is hash partitioned by `id`.
          val df1 = spark.read.table("t")
          val df2 =
            spark
              .range(0, 1000, 1, numInputPartitions)
              .selectExpr("id % 500 as key2", "id as value2")

          val join = df1.join(df2, col("id") === col("key2")).select(col("id"), col("value2"))

          // Check the answer first.
          val expectedAnswer = spark.range(0, 500).selectExpr("id % 500", "id as value")
            .union(spark.range(500, 1000).selectExpr("id % 500", "id as value"))
          QueryTest.checkAnswer(
            join,
            expectedAnswer.collect())

          // Then, let's make sure we do not reduce number of post shuffle partitions.
          val finalPlan = join.queryExecution.executedPlan
            .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
          val shuffleReaders = finalPlan.collect {
            case r @ CoalescedShuffleReader() => r
          }
          assert(shuffleReaders.length === 0)
        } finally {
          spark.sql("drop table t")
        }
      }
      withSparkSession(test, 12000, minNumPostShufflePartitions)
    }
  }

  test("SPARK-24705 adaptive query execution works correctly when exchange reuse enabled") {
    val test: SparkSession => Unit = { spark: SparkSession =>
      spark.sql("SET spark.sql.exchange.reuse=true")
      val df = spark.range(0, 6, 1).selectExpr("id AS key", "id AS value")

      // test case 1: a query stage has 3 child stages but they are the same stage.
      // Final Stage 1
      //   ShuffleQueryStage 0
      //   ReusedQueryStage 0
      //   ReusedQueryStage 0
      val resultDf = df.join(df, "key").join(df, "key")
      QueryTest.checkAnswer(resultDf, (0 to 5).map(i => Row(i, i, i, i)))
      val finalPlan = resultDf.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      assert(finalPlan.collect {
        case ShuffleQueryStageExec(_, r: ReusedExchangeExec, _) => r
      }.length == 2)
      assert(
        finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }.length == 3)


      // test case 2: a query stage has 2 parent stages.
      // Final Stage 3
      //   ShuffleQueryStage 1
      //     ShuffleQueryStage 0
      //   ShuffleQueryStage 2
      //     ReusedQueryStage 0
      val grouped = df.groupBy("key").agg(max("value").as("value"))
      val resultDf2 = grouped.groupBy(col("key") + 1).max("value")
        .union(grouped.groupBy(col("key") + 2).max("value"))
      QueryTest.checkAnswer(resultDf2, Row(1, 0) :: Row(2, 0) :: Row(2, 1) :: Row(3, 1) ::
        Row(3, 2) :: Row(4, 2) :: Row(4, 3) :: Row(5, 3) :: Row(5, 4) :: Row(6, 4) :: Row(6, 5) ::
        Row(7, 5) :: Nil)

      val finalPlan2 = resultDf2.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan

      // The result stage has 2 children
      val level1Stages = finalPlan2.collect { case q: QueryStageExec => q }
      assert(level1Stages.length == 2)

      assert(
        finalPlan2.collect {
          case r @ CoalescedShuffleReader() => r
        }.length == 2, "finalPlan2")

      level1Stages.foreach(qs =>
        assert(qs.plan.collect {
          case r @ CoalescedShuffleReader() => r
        }.length == 1,
          "Wrong CoalescedShuffleReader below " + qs.simpleString(3)))

      val leafStages = level1Stages.flatMap { stage =>
        // All of the child stages of result stage have only one child stage.
        val children = stage.plan.collect { case q: QueryStageExec => q }
        assert(children.length == 1)
        children
      }
      assert(leafStages.length == 2)

      val reusedStages = level1Stages.flatMap { stage =>
        stage.plan.collect {
          case ShuffleQueryStageExec(_, r: ReusedExchangeExec, _) => r
        }
      }
      assert(reusedStages.length == 1)
    }
    withSparkSession(test, 400, None)
  }

  test("Do not reduce the number of shuffle partition for repartition") {
    val test: SparkSession => Unit = { spark: SparkSession =>
      val ds = spark.range(3)
      val resultDf = ds.repartition(2, ds.col("id")).toDF()

      QueryTest.checkAnswer(resultDf,
        Seq(0, 1, 2).map(i => Row(i)))
      val finalPlan = resultDf.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      assert(
        finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }.isEmpty)
    }
    withSparkSession(test, 200, None)
  }

  test("Union two datasets with different pre-shuffle partition number") {
    val test: SparkSession => Unit = { spark: SparkSession =>
      val df1 = spark.range(3).join(spark.range(3), "id").toDF()
      val df2 = spark.range(3).groupBy().sum()

      val resultDf = df1.union(df2)

      QueryTest.checkAnswer(resultDf, Seq((0), (1), (2), (3)).map(i => Row(i)))

      val finalPlan = resultDf.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      // As the pre-shuffle partition number are different, we will skip reducing
      // the shuffle partition numbers.
      assert(
        finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }.isEmpty)
    }
    withSparkSession(test, 100, None)
  }

  test("SPARK-34790: enable IO encryption in AQE partition coalescing") {
    val test: SparkSession => Unit = { spark: SparkSession =>
      val ds = spark.range(0, 100, 1, numInputPartitions)
      val resultDf = ds.repartition(ds.col("id"))
      resultDf.collect()

      val finalPlan = resultDf.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      assert(
        finalPlan.collect {
          case r @ CoalescedShuffleReader() => r
        }.isDefinedAt(0))
    }
    Seq(true, false).foreach { enableIOEncryption =>
      // Before SPARK-34790, it will throw an exception when io encryption enabled.
      withSparkSession(test, Int.MaxValue, None, enableIOEncryption)
    }
  }
}

object CoalescedShuffleReader {
  def unapply(reader: CustomShuffleReaderExec): Boolean = {
    !reader.isLocalReader && !reader.hasSkewedPartition && reader.hasCoalescedPartition
  }
}
