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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildRight, SortMergeJoinExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
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
              "AdaptiveSparkPlan(isFinalPlan=true)")) {
              finalPlanCnt += 1
            }
          case _ => // ignore other events
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    val dfAdaptive = sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan(isFinalPlan=false)"))
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      QueryTest.sameRows(result.toSeq, df.collect().toSeq)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan(isFinalPlan=true)"))

    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(finalPlanCnt == 1)
    spark.sparkContext.removeSparkListener(listener)

    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
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
    collectInPlanAndSubqueries(plan) {
      case ShuffleQueryStageExec(_, e: ReusedExchangeExec) => e
      case BroadcastQueryStageExec(_, e: ReusedExchangeExec) => e
    }
  }

  private def findReusedSubquery(plan: SparkPlan): Seq[ReusedSubqueryExec] = {
    collectInPlanAndSubqueries(plan) {
      case e: ReusedSubqueryExec => e
    }
  }

  private def checkNumLocalShuffleReaders(
      plan: SparkPlan, numShufflesWithoutLocalReader: Int = 0): Unit = {
    val numShuffles = collect(plan) {
      case s: ShuffleQueryStageExec => s
    }.length

    val numLocalReaders = collect(plan) {
      case reader: LocalShuffleReaderExec => reader
    }.length

    assert(numShuffles === (numLocalReaders + numShufflesWithoutLocalReader))
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
      SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key -> "10") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      val localReaders = collect(adaptivePlan) {
        case reader: LocalShuffleReaderExec => reader
      }
      assert(localReaders.length == 2)
      val localShuffleRDD0 = localReaders(0).execute().asInstanceOf[LocalShuffledRowRDD]
      val localShuffleRDD1 = localReaders(1).execute().asInstanceOf[LocalShuffledRowRDD]
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
      SQLConf.REDUCE_POST_SHUFFLE_PARTITIONS_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      val localReaders = collect(adaptivePlan) {
        case reader: LocalShuffleReaderExec => reader
      }
      assert(localReaders.length == 2)
      val localShuffleRDD0 = localReaders(0).execute().asInstanceOf[LocalShuffledRowRDD]
      val localShuffleRDD1 = localReaders(1).execute().asInstanceOf[LocalShuffledRowRDD]
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
        assert(error.getCause().toString contains "Failed to materialize query stage")
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
}
