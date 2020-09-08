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

package org.apache.spark.sql.execution.adaptive

import java.io.File
import java.net.URI

import org.apache.log4j.Level

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.{QueryTest, Row, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{PartialReducerPartitionSpec, ReusedSubqueryExec, ShuffledRowRDD, SparkPlan}
import org.apache.spark.sql.execution.adaptive.OptimizeLocalShuffleReader.LOCAL_SHUFFLE_READER_DESCRIPTION
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildRight, SortMergeJoinExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.util.Utils

class AdaptiveQueryExecSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  setupTestData()

  private def runAdaptiveAndVerifyResult(query: String): (SparkPlan, SparkPlan) = {
    var finalPlanCnt = 0
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
            if (sparkPlanInfo.simpleString.startsWith(
              "AdaptiveSparkPlan isFinalPlan=true")) {
              finalPlanCnt += 1
            }
          case _ => // ignore other events
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    val dfAdaptive = sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, result)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    spark.sparkContext.listenerBus.waitUntilEmpty()
    // AQE will post `SparkListenerSQLAdaptiveExecutionUpdate` twice in case of subqueries that
    // exist out of query stages.
    val expectedFinalPlanCnt = adaptivePlan.find(_.subqueries.nonEmpty).map(_ => 2).getOrElse(1)
    assert(finalPlanCnt == expectedFinalPlanCnt)
    spark.sparkContext.removeSparkListener(listener)

    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findTopLevelBroadcastHashJoin(plan: SparkPlan): Seq[BroadcastHashJoinExec] = {
    collect(plan) {
      case j: BroadcastHashJoinExec => j
    }
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] = {
    collect(plan) {
      case j: SortMergeJoinExec => j
    }
  }

  private def findReusedExchange(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    collectWithSubqueries(plan) {
      case ShuffleQueryStageExec(_, e: ReusedExchangeExec) => e
      case BroadcastQueryStageExec(_, e: ReusedExchangeExec) => e
    }
  }

  private def findReusedSubquery(plan: SparkPlan): Seq[ReusedSubqueryExec] = {
    collectWithSubqueries(plan) {
      case e: ReusedSubqueryExec => e
    }
  }

  private def checkNumLocalShuffleReaders(
      plan: SparkPlan, numShufflesWithoutLocalReader: Int = 0): Unit = {
    val numShuffles = collect(plan) {
      case s: ShuffleQueryStageExec => s
    }.length

    val numLocalReaders = collect(plan) {
      case reader @ CustomShuffleReaderExec(_, _, LOCAL_SHUFFLE_READER_DESCRIPTION) => reader
    }
    numLocalReaders.foreach { r =>
      val rdd = r.execute()
      val parts = rdd.partitions
      assert(parts.forall(rdd.preferredLocations(_).nonEmpty))
    }
    assert(numShuffles === (numLocalReaders.length + numShufflesWithoutLocalReader))
  }

  test("Change merge join to broadcast join") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReaders(adaptivePlan)
    }
  }

  test("Reuse the parallelism of CoalescedShuffleReaderExec in LocalShuffleReaderExec") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      val localReaders = collect(adaptivePlan) {
        case reader @ CustomShuffleReaderExec(_, _, LOCAL_SHUFFLE_READER_DESCRIPTION) => reader
      }
      assert(localReaders.length == 2)
      val localShuffleRDD0 = localReaders(0).execute().asInstanceOf[ShuffledRowRDD]
      val localShuffleRDD1 = localReaders(1).execute().asInstanceOf[ShuffledRowRDD]
      // The pre-shuffle partition size is [0, 0, 0, 72, 0]
      // And the partitionStartIndices is [0, 3, 4], so advisoryParallelism = 3.
      // the final parallelism is
      // math.max(1, advisoryParallelism / numMappers): math.max(1, 3/2) = 1
      // and the partitions length is 1 * numMappers = 2
      assert(localShuffleRDD0.getPartitions.length == 2)
      // The pre-shuffle partition size is [0, 72, 0, 72, 126]
      // And the partitionStartIndices is [0, 1, 2, 3, 4], so advisoryParallelism = 5.
      // the final parallelism is
      // math.max(1, advisoryParallelism / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD1.getPartitions.length == 4)
    }
  }

  test("Reuse the default parallelism in LocalShuffleReaderExec") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      val localReaders = collect(adaptivePlan) {
        case reader @ CustomShuffleReaderExec(_, _, LOCAL_SHUFFLE_READER_DESCRIPTION) => reader
      }
      assert(localReaders.length == 2)
      val localShuffleRDD0 = localReaders(0).execute().asInstanceOf[ShuffledRowRDD]
      val localShuffleRDD1 = localReaders(1).execute().asInstanceOf[ShuffledRowRDD]
      // the final parallelism is math.max(1, numReduces / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD0.getPartitions.length == 4)
      // the final parallelism is math.max(1, numReduces / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD1.getPartitions.length == 4)
    }
  }

  test("Scalar subquery") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a " +
        "where value = (SELECT max(a) from testData3)")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReaders(adaptivePlan)
    }
  }

  test("Scalar subquery in later stages") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a " +
        "where (value + a) = (SELECT max(a) from testData3)")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)

      checkNumLocalShuffleReaders(adaptivePlan)
    }
  }

  test("multiple joins") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN testData3 t3 ON t2.n = t3.a where t2.n = '1'
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON t2.b = t4.a
          |WHERE value = 1
        """.stripMargin)
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 3)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 3)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- BroadcastExchange
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //          +- BroadcastHashJoin
      //             +- BroadcastExchange
      //                +- LocalShuffleReader*
      //                   +- ShuffleExchange
      //             +- LocalShuffleReader*
      //                +- ShuffleExchange
      // +- BroadcastHashJoin
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //    +- BroadcastExchange
      //       +-LocalShuffleReader*
      //             +- ShuffleExchange

      // After applied the 'OptimizeLocalShuffleReader' rule, we can convert all the four
      // shuffle reader to local shuffle reader in the bottom two 'BroadcastHashJoin'.
      // For the top level 'BroadcastHashJoin', the probe side is not shuffle query stage
      // and the build side shuffle query stage is also converted to local shuffle reader.
      checkNumLocalShuffleReaders(adaptivePlan)
    }
  }

  test("multiple joins with aggregate") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN (
          |    select a, sum(b) from testData3 group by a
          |  ) t3 ON t2.n = t3.a where t2.n = '1'
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON t2.b = t4.a
          |WHERE value = 1
        """.stripMargin)
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 3)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 3)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- BroadcastExchange
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //          +- BroadcastHashJoin
      //             +- BroadcastExchange
      //                +- LocalShuffleReader*
      //                   +- ShuffleExchange
      //             +- LocalShuffleReader*
      //                +- ShuffleExchange
      // +- BroadcastHashJoin
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //    +- BroadcastExchange
      //       +-HashAggregate
      //          +- CoalescedShuffleReader
      //             +- ShuffleExchange

      // The shuffle added by Aggregate can't apply local reader.
      checkNumLocalShuffleReaders(adaptivePlan, 1)
    }
  }

  test("multiple joins with aggregate 2") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN (
          |    select a, max(b) b from testData2 group by a
          |  ) t3 ON t2.n = t3.b
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON value = t4.a
          |WHERE value = 1
        """.stripMargin)
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 3)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 3)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- BroadcastExchange
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //          +- BroadcastHashJoin
      //             +- BroadcastExchange
      //                +- LocalShuffleReader*
      //                   +- ShuffleExchange
      //             +- LocalShuffleReader*
      //                +- ShuffleExchange
      // +- BroadcastHashJoin
      //    +- Filter
      //       +- HashAggregate
      //          +- CoalescedShuffleReader
      //             +- ShuffleExchange
      //    +- BroadcastExchange
      //       +-LocalShuffleReader*
      //           +- ShuffleExchange

      // The shuffle added by Aggregate can't apply local reader.
      checkNumLocalShuffleReaders(adaptivePlan, 1)
    }
  }

  test("Exchange reuse") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT value FROM testData join testData2 ON key = a " +
        "join (SELECT value v from testData join testData3 ON key = a) on value = v")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 3)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 2)
      // There is still a SMJ, and its two shuffles can't apply local reader.
      checkNumLocalShuffleReaders(adaptivePlan, 2)
      // Even with local shuffle reader, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.size == 1)
    }
  }

  test("Exchange reuse with subqueries") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
        "where value = (SELECT max(a) from testData join testData2 ON key = a)")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReaders(adaptivePlan)
      // Even with local shuffle reader, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.size == 1)
    }
  }

  test("Exchange reuse across subqueries") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80",
        SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
        "where value >= (SELECT max(a) from testData join testData2 ON key = a) " +
        "and a <= (SELECT max(a) from testData join testData2 ON key = a)")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReaders(adaptivePlan)
      // Even with local shuffle reader, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.nonEmpty)
      val sub = findReusedSubquery(adaptivePlan)
      assert(sub.isEmpty)
    }
  }

  test("Subquery reuse") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
        "where value >= (SELECT max(a) from testData join testData2 ON key = a) " +
        "and a <= (SELECT max(a) from testData join testData2 ON key = a)")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReaders(adaptivePlan)
      // Even with local shuffle reader, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.isEmpty)
      val sub = findReusedSubquery(adaptivePlan)
      assert(sub.nonEmpty)
    }
  }

  test("Broadcast exchange reuse across subqueries") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "20000000",
        SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
        "where value >= (" +
        "SELECT /*+ broadcast(testData2) */ max(key) from testData join testData2 ON key = a) " +
        "and a <= (" +
        "SELECT /*+ broadcast(testData2) */ max(value) from testData join testData2 ON key = a)")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReaders(adaptivePlan)
      // Even with local shuffle reader, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.nonEmpty)
      assert(ex.head.child.isInstanceOf[BroadcastExchangeExec])
      val sub = findReusedSubquery(adaptivePlan)
      assert(sub.isEmpty)
    }
  }

  test("Union/Except/Intersect queries") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      runAdaptiveAndVerifyResult(
        """
          |SELECT * FROM testData
          |EXCEPT
          |SELECT * FROM testData2
          |UNION ALL
          |SELECT * FROM testData
          |INTERSECT ALL
          |SELECT * FROM testData2
        """.stripMargin)
    }
  }

  test("Subquery de-correlation in Union queries") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("a", "b") {
        Seq("a" -> 2, "b" -> 1).toDF("id", "num").createTempView("a")
        Seq("a" -> 2, "b" -> 1).toDF("id", "num").createTempView("b")

        runAdaptiveAndVerifyResult(
          """
            |SELECT id,num,source FROM (
            |  SELECT id, num, 'a' as source FROM a
            |  UNION ALL
            |  SELECT id, num, 'b' as source FROM b
            |) AS c WHERE c.id IN (SELECT id FROM b WHERE num = 2)
          """.stripMargin)
      }
    }
  }

  test("Avoid plan change if cost is greater") {
    val origPlan = sql("SELECT * FROM testData " +
      "join testData2 t2 ON key = t2.a " +
      "join testData2 t3 on t2.a = t3.a where t2.b = 1").queryExecution.executedPlan

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData " +
          "join testData2 t2 ON key = t2.a " +
          "join testData2 t3 on t2.a = t3.a where t2.b = 1")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 2)
      val smj2 = findTopLevelSortMergeJoin(adaptivePlan)
      assert(smj2.size == 2, origPlan.toString)
    }
  }

  test("Change merge join to broadcast join without local shuffle reader") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "40") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |SELECT * FROM testData t1 join testData2 t2
          |ON t1.key = t2.a join testData3 t3 on t2.a = t3.a
          |where t1.value = 1
        """.stripMargin
      )
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 2)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      // There is still a SMJ, and its two shuffles can't apply local reader.
      checkNumLocalShuffleReaders(adaptivePlan, 2)
    }
  }

  test("Avoid changing merge join to broadcast join if too many empty partitions on build plan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key -> "0.5") {
      // `testData` is small enough to be broadcast but has empty partition ratio over the config.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM testData join testData2 ON key = a where value = '1'")
        val smj = findTopLevelSortMergeJoin(plan)
        assert(smj.size == 1)
        val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
        assert(bhj.isEmpty)
      }
      // It is still possible to broadcast `testData2`.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM testData join testData2 ON key = a where value = '1'")
        val smj = findTopLevelSortMergeJoin(plan)
        assert(smj.size == 1)
        val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
        assert(bhj.size == 1)
        assert(bhj.head.buildSide == BuildRight)
      }
    }
  }

  test("SPARK-29906: AQE should not introduce extra shuffle for outermost limit") {
    var numStages = 0
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        numStages = jobStart.stageInfos.length
      }
    }
    try {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        spark.sparkContext.addSparkListener(listener)
        spark.range(0, 100, 1, numPartitions = 10).take(1)
        spark.sparkContext.listenerBus.waitUntilEmpty()
        // Should be only one stage since there is no shuffle.
        assert(numStages == 1)
      }
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("SPARK-30524: Do not optimize skew join if introduce additional shuffle") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100") {
      withTempView("skewData1", "skewData2") {
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 3 as key1", "id as value1")
          .createOrReplaceTempView("skewData1")
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 1 as key2", "id as value2")
          .createOrReplaceTempView("skewData2")

        def checkSkewJoin(query: String, optimizeSkewJoin: Boolean): Unit = {
          val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(query)
          val innerSmj = findTopLevelSortMergeJoin(innerAdaptivePlan)
          assert(innerSmj.size == 1 && innerSmj.head.isSkewJoin == optimizeSkewJoin)
        }

        checkSkewJoin(
          "SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2", true)
        // Additional shuffle introduced, so disable the "OptimizeSkewedJoin" optimization
        checkSkewJoin(
          "SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 GROUP BY key1", false)
      }
    }
  }

  test("SPARK-29544: adaptive skew join with different join types") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "100",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "800",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "800") {
      withTempView("skewData1", "skewData2") {
        spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 250, 249)
              .when('id >= 750, 1000)
              .otherwise('id).as("key1"),
            'id as "value1")
          .createOrReplaceTempView("skewData1")
        spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 250, 249)
              .otherwise('id).as("key2"),
            'id as "value2")
          .createOrReplaceTempView("skewData2")

        def checkSkewJoin(
            joins: Seq[SortMergeJoinExec],
            leftSkewNum: Int,
            rightSkewNum: Int): Unit = {
          assert(joins.size == 1 && joins.head.isSkewJoin)
          assert(joins.head.left.collect {
            case r: CustomShuffleReaderExec => r
          }.head.partitionSpecs.collect {
            case p: PartialReducerPartitionSpec => p.reducerIndex
          }.distinct.length == leftSkewNum)
          assert(joins.head.right.collect {
            case r: CustomShuffleReaderExec => r
          }.head.partitionSpecs.collect {
            case p: PartialReducerPartitionSpec => p.reducerIndex
          }.distinct.length == rightSkewNum)
        }

        // skewed inner join optimization
        val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM skewData1 join skewData2 ON key1 = key2")
        val innerSmj = findTopLevelSortMergeJoin(innerAdaptivePlan)
        checkSkewJoin(innerSmj, 2, 1)

        // skewed left outer join optimization
        val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM skewData1 left outer join skewData2 ON key1 = key2")
        val leftSmj = findTopLevelSortMergeJoin(leftAdaptivePlan)
        checkSkewJoin(leftSmj, 2, 0)

        // skewed right outer join optimization
        val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM skewData1 right outer join skewData2 ON key1 = key2")
        val rightSmj = findTopLevelSortMergeJoin(rightAdaptivePlan)
        checkSkewJoin(rightSmj, 0, 1)
      }
    }
  }

  test("SPARK-30291: AQE should catch the exceptions when doing materialize") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTable("bucketed_table") {
        val df1 =
          (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k").as("df1")
        df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
        val warehouseFilePath = new URI(spark.sessionState.conf.warehousePath).getPath
        val tableDir = new File(warehouseFilePath, "bucketed_table")
        Utils.deleteRecursively(tableDir)
        df1.write.parquet(tableDir.getAbsolutePath)

        val agged = spark.table("bucketed_table").groupBy("i").count()
        val error = intercept[Exception] {
          agged.count()
        }
        assert(error.getCause().toString contains "Invalid bucket file")
        assert(error.getSuppressed.size === 0)
      }
    }
  }

  test("SPARK-30403: AQE should handle InSubquery") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      runAdaptiveAndVerifyResult("SELECT * FROM testData LEFT OUTER join testData2" +
        " ON key = a  AND key NOT IN (select a from testData3) where value = '1'"
      )
    }
  }

  test("force apply AQE") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      val plan = sql("SELECT * FROM testData").queryExecution.executedPlan
      assert(plan.isInstanceOf[AdaptiveSparkPlanExec])
    }
  }

  test("SPARK-30719: do not log warning if intentionally skip AQE") {
    val testAppender = new LogAppender("aqe logging warning test when skip")
    withLogAppender(testAppender) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        val plan = sql("SELECT * FROM testData").queryExecution.executedPlan
        assert(!plan.isInstanceOf[AdaptiveSparkPlanExec])
      }
    }
    assert(!testAppender.loggingEvents
      .exists(msg => msg.getRenderedMessage.contains(
        s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is" +
        s" enabled but is not supported for")))
  }

  test("test log level") {
    def verifyLog(expectedLevel: Level): Unit = {
      val logAppender = new LogAppender("adaptive execution")
      withLogAppender(
        logAppender,
        loggerName = Some(AdaptiveSparkPlanExec.getClass.getName.dropRight(1)),
        level = Some(Level.TRACE)) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
          sql("SELECT * FROM testData join testData2 ON key = a where value = '1'").collect()
        }
      }
      Seq("Plan changed", "Final plan").foreach { msg =>
        assert(
          logAppender.loggingEvents.exists { event =>
            event.getRenderedMessage.contains(msg) && event.getLevel == expectedLevel
          })
      }
    }

    // Verify default log level
    verifyLog(Level.DEBUG)

    // Verify custom log level
    val levels = Seq(
      "TRACE" -> Level.TRACE,
      "trace" -> Level.TRACE,
      "DEBUG" -> Level.DEBUG,
      "debug" -> Level.DEBUG,
      "INFO" -> Level.INFO,
      "info" -> Level.INFO,
      "WARN" -> Level.WARN,
      "warn" -> Level.WARN,
      "ERROR" -> Level.ERROR,
      "error" -> Level.ERROR,
      "deBUG" -> Level.DEBUG)

    levels.foreach { level =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_LOG_LEVEL.key -> level._1) {
        verifyLog(level._2)
      }
    }
  }

  test("SPARK-31384: avoid NPE in OptimizeSkewedJoin when there's 0 partition plan") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("t2") {
        // create DataFrame with 0 partition
        spark.createDataFrame(sparkContext.emptyRDD[Row], new StructType().add("b", IntegerType))
          .createOrReplaceTempView("t2")
        // should run successfully without NPE
        runAdaptiveAndVerifyResult("SELECT * FROM testData2 t1 left semi join t2 ON t1.a=t2.b")
      }
    }
  }

  test("SPARK-30953: InsertAdaptiveSparkPlan should apply AQE on child plan of write commands") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      withTable("t1") {
        val plan = sql("CREATE TABLE t1 USING parquet AS SELECT 1 col").queryExecution.executedPlan
        assert(plan.isInstanceOf[DataWritingCommandExec])
        assert(plan.asInstanceOf[DataWritingCommandExec].child.isInstanceOf[AdaptiveSparkPlanExec])
      }
    }
  }

  test("AQE should set active session during execution") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = spark.range(10).select(sum('id))
      assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      SparkSession.setActiveSession(null)
      checkAnswer(df, Seq(Row(45)))
      SparkSession.setActiveSession(spark) // recover the active session.
    }
  }

  test("No deadlock in UI update") {
    object TestStrategy extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case _: Aggregate =>
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
            spark.range(5).rdd
          }
          Nil
        case _ => Nil
      }
    }

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      try {
        spark.experimental.extraStrategies = TestStrategy :: Nil
        val df = spark.range(10).groupBy('id).count()
        df.collect()
      } finally {
        spark.experimental.extraStrategies = Nil
      }
    }
  }

  test("SPARK-31658: SQL UI should show write commands") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      withTable("t1") {
        var checkDone = false
        val listener = new SparkListener {
          override def onOtherEvent(event: SparkListenerEvent): Unit = {
            event match {
              case SparkListenerSQLAdaptiveExecutionUpdate(_, _, planInfo) =>
                assert(planInfo.nodeName == "Execute CreateDataSourceTableAsSelectCommand")
                checkDone = true
              case _ => // ignore other events
            }
          }
        }
        spark.sparkContext.addSparkListener(listener)
        try {
          sql("CREATE TABLE t1 USING parquet AS SELECT 1 col").collect()
          spark.sparkContext.listenerBus.waitUntilEmpty()
          assert(checkDone)
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }
  }

  test("SPARK-31220 repartition obeys initialPartitionNum when adaptiveExecutionEnabled") {
    Seq(true, false).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "6",
        SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "7") {
        val partitionsNum = spark.range(10).repartition($"id").rdd.collectPartitions().length
        if (enableAQE) {
          assert(partitionsNum === 7)
        } else {
          assert(partitionsNum === 6)
        }
      }
    }
  }

  test("SPARK-32753: Only copy tags to node with no tags") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("v1") {
        spark.range(10).union(spark.range(10)).createOrReplaceTempView("v1")

        val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT id FROM v1 GROUP BY id DISTRIBUTE BY id")
        assert(collect(adaptivePlan) {
          case s: ShuffleExchangeExec => s
        }.length == 1)
      }
    }
  }
}
