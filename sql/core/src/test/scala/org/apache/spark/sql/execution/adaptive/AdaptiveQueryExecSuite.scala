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

import org.apache.logging.log4j.Level
import org.scalatest.PrivateMethodTester
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{CollectLimitExec, ColumnarToRowExec, EmptyRelationExec, PartialReducerPartitionSpec, QueryExecution, ReusedSubqueryExec, ShuffledRowRDD, SortExec, SparkPlan, SparkPlanInfo, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.columnar.{InMemoryTableScanExec, InMemoryTableScanLike}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.noop.NoopDataSource
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ENSURE_REQUIREMENTS, Exchange, REPARTITION_BY_COL, REPARTITION_BY_NUM, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

@SlowSQLTest
class AdaptiveQueryExecSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper
  with PrivateMethodTester {

  import testImplicits._

  setupTestData()

  private def runAdaptiveAndVerifyResult(query: String,
      skipCheckAnswer: Boolean = false): (SparkPlan, SparkPlan) = {
    var finalPlanCnt = 0
    var hasMetricsEvent = false
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
            if (sparkPlanInfo.simpleString.startsWith(
              "AdaptiveSparkPlan isFinalPlan=true")) {
              finalPlanCnt += 1
            }
          case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
            hasMetricsEvent = true
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
      if (!skipCheckAnswer) {
        val df = sql(query)
        checkAnswer(df, result.toImmutableArraySeq)
      }
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

    val expectedMetrics = findInMemoryTable(planAfter).nonEmpty ||
      subqueriesAll(planAfter).nonEmpty
    assert(hasMetricsEvent == expectedMetrics)

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

  def findTopLevelBroadcastNestedLoopJoin(plan: SparkPlan): Seq[BaseJoinExec] = {
    collect(plan) {
      case j: BroadcastNestedLoopJoinExec => j
    }
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] = {
    collect(plan) {
      case j: SortMergeJoinExec => j
    }
  }

  private def findTopLevelShuffledHashJoin(plan: SparkPlan): Seq[ShuffledHashJoinExec] = {
    collect(plan) {
      case j: ShuffledHashJoinExec => j
    }
  }

  private def findTopLevelBaseJoin(plan: SparkPlan): Seq[BaseJoinExec] = {
    collect(plan) {
      case j: BaseJoinExec => j
    }
  }

  private def findTopLevelSort(plan: SparkPlan): Seq[SortExec] = {
    collect(plan) {
      case s: SortExec => s
    }
  }

  private def findTopLevelAggregate(plan: SparkPlan): Seq[BaseAggregateExec] = {
    collect(plan) {
      case agg: BaseAggregateExec => agg
    }
  }

  private def findTopLevelLimit(plan: SparkPlan): Seq[CollectLimitExec] = {
    collect(plan) {
      case l: CollectLimitExec => l
    }
  }

  private def findTopLevelUnion(plan: SparkPlan): Seq[UnionExec] = {
    collect(plan) {
      case l: UnionExec => l
    }
  }

  private def findReusedExchange(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    collectWithSubqueries(plan) {
      case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _) => e
      case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
    }
  }

  private def findReusedSubquery(plan: SparkPlan): Seq[ReusedSubqueryExec] = {
    collectWithSubqueries(plan) {
      case e: ReusedSubqueryExec => e
    }
  }

  private def findInMemoryTable(plan: SparkPlan): Seq[InMemoryTableScanExec] = {
    collect(plan) {
      case c: InMemoryTableScanExec
          if c.relation.cachedPlan.isInstanceOf[AdaptiveSparkPlanExec] => c
    }
  }

  private def checkNumLocalShuffleReads(
      plan: SparkPlan, numShufflesWithoutLocalRead: Int = 0): Unit = {
    val numShuffles = collect(plan) {
      case s: ShuffleQueryStageExec => s
    }.length

    val numLocalReads = collect(plan) {
      case read: AQEShuffleReadExec if read.isLocalRead => read
    }
    numLocalReads.foreach { r =>
      val rdd = r.execute()
      val parts = rdd.partitions
      assert(parts.forall(rdd.preferredLocations(_).nonEmpty))
    }
    assert(numShuffles === (numLocalReads.length + numShufflesWithoutLocalRead))
  }

  private def checkInitialPartitionNum(df: Dataset[_], numPartition: Int): Unit = {
    // repartition obeys initialPartitionNum when adaptiveExecutionEnabled
    val plan = df.queryExecution.executedPlan
    assert(plan.isInstanceOf[AdaptiveSparkPlanExec])
    val shuffle = plan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.collect {
      case s: ShuffleExchangeExec => s
    }
    assert(shuffle.size == 1)
    assert(shuffle(0).outputPartitioning.numPartitions == numPartition)
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
      checkNumLocalShuffleReads(adaptivePlan)
    }
  }

  test("Change broadcast join to merge join") {
    withTable("t1", "t2") {
      withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        sql("CREATE TABLE t1 USING PARQUET AS SELECT 1 c1")
        sql("CREATE TABLE t2 USING PARQUET AS SELECT 1 c1")
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          """
            |SELECT * FROM (
            | SELECT distinct c1 from t1
            | ) tmp1 JOIN (
            |  SELECT distinct c1 from t2
            | ) tmp2 ON tmp1.c1 = tmp2.c1
            |""".stripMargin)
        assert(findTopLevelBroadcastHashJoin(plan).size == 1)
        assert(findTopLevelBroadcastHashJoin(adaptivePlan).isEmpty)
        assert(findTopLevelSortMergeJoin(adaptivePlan).size == 1)
      }
    }
  }

  test("Reuse the parallelism of coalesced shuffle in local shuffle read") {
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
      val localReads = collect(adaptivePlan) {
        case read: AQEShuffleReadExec if read.isLocalRead => read
      }
      assert(localReads.length == 2)
      val localShuffleRDD0 = localReads(0).execute().asInstanceOf[ShuffledRowRDD]
      val localShuffleRDD1 = localReads(1).execute().asInstanceOf[ShuffledRowRDD]
      // The pre-shuffle partition size is [0, 0, 0, 72, 0]
      // We exclude the 0-size partitions, so only one partition, advisoryParallelism = 1
      // the final parallelism is
      // advisoryParallelism = 1 since advisoryParallelism < numMappers
      // and the partitions length is 1
      assert(localShuffleRDD0.getPartitions.length == 1)
      // The pre-shuffle partition size is [0, 72, 0, 72, 126]
      // We exclude the 0-size partitions, so only 3 partition, advisoryParallelism = 3
      // the final parallelism is
      // advisoryParallelism / numMappers: 3/2 = 1 since advisoryParallelism >= numMappers
      // and the partitions length is 1 * numMappers = 2
      assert(localShuffleRDD1.getPartitions.length == 2)
    }
  }

  test("Reuse the default parallelism in local shuffle read") {
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
      val localReads = collect(adaptivePlan) {
        case read: AQEShuffleReadExec if read.isLocalRead => read
      }
      assert(localReads.length == 2)
      val localShuffleRDD0 = localReads(0).execute().asInstanceOf[ShuffledRowRDD]
      val localShuffleRDD1 = localReads(1).execute().asInstanceOf[ShuffledRowRDD]
      // the final parallelism is math.max(1, numReduces / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD0.getPartitions.length == 4)
      // the final parallelism is math.max(1, numReduces / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD1.getPartitions.length == 4)
    }
  }

  test("Empty stage coalesced to 1-partition RDD") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      val df1 = spark.range(10).withColumn("a", $"id")
      val df2 = spark.range(10).withColumn("b", $"id")
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val testDf = df1.where($"a" > 10)
          .join(df2.where($"b" > 10), Seq("id"), "left_outer")
          .groupBy($"a").count()
        checkAnswer(testDf, Seq())
        val plan = testDf.queryExecution.executedPlan
        assert(find(plan)(_.isInstanceOf[SortMergeJoinExec]).isDefined)
        val coalescedReads = collect(plan) {
          case r: AQEShuffleReadExec => r
        }
        assert(coalescedReads.length == 3)
        coalescedReads.foreach(r => assert(r.partitionSpecs.length == 1))
      }

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
        val testDf = df1.where($"a" > 10)
          .join(df2.where($"b" > 10), Seq("id"), "left_outer")
          .groupBy($"a").count()
        checkAnswer(testDf, Seq())
        val plan = testDf.queryExecution.executedPlan
        assert(find(plan)(_.isInstanceOf[BroadcastHashJoinExec]).isDefined)
        val coalescedReads = collect(plan) {
          case r: AQEShuffleReadExec => r
        }
        assert(coalescedReads.length == 3, s"$plan")
        coalescedReads.foreach(r => assert(r.isLocalRead || r.partitionSpecs.length == 1))
      }
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
      checkNumLocalShuffleReads(adaptivePlan)
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

      checkNumLocalShuffleReads(adaptivePlan)
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

      // After applied the 'OptimizeShuffleWithLocalRead' rule, we can convert all the four
      // shuffle read to local shuffle read in the bottom two 'BroadcastHashJoin'.
      // For the top level 'BroadcastHashJoin', the probe side is not shuffle query stage
      // and the build side shuffle query stage is also converted to local shuffle read.
      checkNumLocalShuffleReads(adaptivePlan)
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

      // The shuffle added by Aggregate can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 1)
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

      // The shuffle added by Aggregate can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 1)
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
      // There is still a SMJ, and its two shuffles can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 2)
      // Even with local shuffle read, the query stage reuse can also work.
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
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
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
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
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
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
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
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
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
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80",
      SQLConf.BROADCAST_HASH_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT.key -> "0") {
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

  test("Change merge join to broadcast join without local shuffle read") {
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
      // There is still a SMJ, and its two shuffles can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 2)
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
  test("SPARK-37753: Allow changing outer join to broadcast join even if too many empty" +
    " partitions on broadcast side") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key -> "0.5") {
      // `testData` is small enough to be broadcast but has empty partition ratio over the config.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM (select * from testData where value = '1') td" +
            " right outer join testData2 ON key = a")
        val smj = findTopLevelSortMergeJoin(plan)
        assert(smj.size == 1)
        val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
        assert(bhj.size == 1)
      }
    }
  }

  test("SPARK-37753: Inhibit broadcast in left outer join when there are many empty" +
    " partitions on outer/left side") {
    // if the right side is completed first and the left side is still being executed,
    // the right side does not know whether there are many empty partitions on the left side,
    // so there is no demote, and then the right side is broadcast in the planning stage.
    // so retry several times here to avoid unit test failure.
    eventually(timeout(15.seconds), interval(500.milliseconds)) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key -> "0.5") {
        // `testData` is small enough to be broadcast but has empty partition ratio over the config.
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200") {
          val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
            "SELECT * FROM (select * from testData where value = '1') td" +
              " left outer join testData2 ON key = a")
          val smj = findTopLevelSortMergeJoin(plan)
          assert(smj.size == 1)
          val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
          assert(bhj.isEmpty)
        }
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
    Seq("SHUFFLE_MERGE", "SHUFFLE_HASH").foreach { joinHint =>
      def getJoinNode(plan: SparkPlan): Seq[ShuffledJoin] = if (joinHint == "SHUFFLE_MERGE") {
        findTopLevelSortMergeJoin(plan)
      } else {
        findTopLevelShuffledHashJoin(plan)
      }
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
              when($"id" < 250, 249)
                .when($"id" >= 750, 1000)
                .otherwise($"id").as("key1"),
              $"id" as "value1")
            .createOrReplaceTempView("skewData1")
          spark
            .range(0, 1000, 1, 10)
            .select(
              when($"id" < 250, 249)
                .otherwise($"id").as("key2"),
              $"id" as "value2")
            .createOrReplaceTempView("skewData2")

          def checkSkewJoin(
              joins: Seq[ShuffledJoin],
              leftSkewNum: Int,
              rightSkewNum: Int): Unit = {
            assert(joins.size == 1 && joins.head.isSkewJoin)
            assert(joins.head.left.collect {
              case r: AQEShuffleReadExec => r
            }.head.partitionSpecs.collect {
              case p: PartialReducerPartitionSpec => p.reducerIndex
            }.distinct.length == leftSkewNum)
            assert(joins.head.right.collect {
              case r: AQEShuffleReadExec => r
            }.head.partitionSpecs.collect {
              case p: PartialReducerPartitionSpec => p.reducerIndex
            }.distinct.length == rightSkewNum)
          }

          // skewed inner join optimization
          val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
            s"SELECT /*+ $joinHint(skewData1) */ * FROM skewData1 " +
              "JOIN skewData2 ON key1 = key2")
          val inner = getJoinNode(innerAdaptivePlan)
          checkSkewJoin(inner, 2, 1)

          // skewed left outer join optimization
          val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
            s"SELECT /*+ $joinHint(skewData2) */ * FROM skewData1 " +
              "LEFT OUTER JOIN skewData2 ON key1 = key2")
          val leftJoin = getJoinNode(leftAdaptivePlan)
          checkSkewJoin(leftJoin, 2, 0)

          // skewed right outer join optimization
          val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
            s"SELECT /*+ $joinHint(skewData1) */ * FROM skewData1 " +
              "RIGHT OUTER JOIN skewData2 ON key1 = key2")
          val rightJoin = getJoinNode(rightAdaptivePlan)
          checkSkewJoin(rightJoin, 0, 1)
        }
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

        val aggregated = spark.table("bucketed_table").groupBy("i").count()
        val error = intercept[SparkException] {
          aggregated.count()
        }
        assert(error.getErrorClass === "INVALID_BUCKET_FILE")
        assert(error.getMessage contains "Invalid bucket file")
      }
    }
  }

  test("SPARK-47148: AQE should avoid to submit shuffle job on cancellation") {
    def createJoinedDF(): DataFrame = {
      // Use subquery expression containing `slow_udf` to delay the submission of shuffle jobs.
      val df = sql("SELECT id, (SELECT slow_udf() FROM range(2)) FROM range(5)")
      val df2 = sql("SELECT id FROM range(10)").coalesce(2)
      val df3 = sql("SELECT id, (SELECT slow_udf() FROM range(2)) FROM range(15) WHERE id > 2")
      df.join(df2, Seq("id")).join(df3, Seq("id"))
    }

    withUserDefinedFunction("slow_udf" -> true) {
      spark.udf.register("slow_udf", () => {
        Thread.sleep(3000)
        1
      })

      try {
        spark.experimental.extraStrategies = TestProblematicCoalesceStrategy :: Nil
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val joined = createJoinedDF()
          joined.explain(true)

          val error = intercept[SparkException] {
            joined.collect()
          }
          assert(error.getMessage() contains "coalesce test error")

          val adaptivePlan = joined.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]

          // All QueryStages should be based on ShuffleQueryStageExec
          val shuffleQueryStageExecs = collect(adaptivePlan) {
            case sqse: ShuffleQueryStageExec => sqse
          }
          assert(shuffleQueryStageExecs.length == 3, s"Physical Plan should include " +
            s"3 ShuffleQueryStages. Physical Plan: $adaptivePlan")
          // First ShuffleQueryStage is cancelled before shuffle job is submitted.
          assert(shuffleQueryStageExecs(0).shuffle.futureAction.get.isEmpty)
          // Second ShuffleQueryStage has submitted the shuffle job but it failed.
          assert(shuffleQueryStageExecs(1).shuffle.futureAction.get.isDefined,
            "Materialization should be started but it is failed.")
          // Third ShuffleQueryStage is cancelled before shuffle job is submitted.
          assert(shuffleQueryStageExecs(2).shuffle.futureAction.get.isEmpty)
        }
      } finally {
        spark.experimental.extraStrategies = Nil
      }
    }
  }

  test("SPARK-30403: AQE should handle InSubquery") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DECORRELATE_PREDICATE_SUBQUERIES_IN_JOIN_CONDITION.key -> "false") {
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
      .exists(msg => msg.getMessage.getFormattedMessage.contains(
        s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is" +
        s" enabled but is not supported for")))
  }

  test("test log level") {
    def verifyLog(expectedLevel: Level): Unit = {
      val logAppender = new LogAppender("adaptive execution")
      logAppender.setThreshold(expectedLevel)
      withLogAppender(
        logAppender,
        loggerNames = Seq(AdaptiveSparkPlanExec.getClass.getName.dropRight(1)),
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
            event.getMessage.getFormattedMessage.contains(msg) && event.getLevel == expectedLevel
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

  test("tree string output") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = sql("SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val planBefore = df.queryExecution.executedPlan
      assert(!planBefore.toString.contains("== Current Plan =="))
      assert(!planBefore.toString.contains("== Initial Plan =="))
      df.collect()
      val planAfter = df.queryExecution.executedPlan
      assert(planAfter.toString.contains("== Final Plan =="))
      assert(planAfter.toString.contains("== Initial Plan =="))
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

  test("SPARK-34682: AQEShuffleReadExec operating on canonicalized plan") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT key FROM testData GROUP BY key")
      val reads = collect(adaptivePlan) {
        case r: AQEShuffleReadExec => r
      }
      assert(reads.length == 1)
      val read = reads.head
      val c = read.canonicalized.asInstanceOf[AQEShuffleReadExec]
      // we can't just call execute() because that has separate checks for canonicalized plans
      checkError(
        exception = intercept[SparkException] {
          val doExecute = PrivateMethod[Unit](Symbol("doExecute"))
          c.invokePrivate(doExecute())
        },
        errorClass = "INTERNAL_ERROR",
        parameters = Map("message" -> "operating on canonicalized plan"))
    }
  }

  test("metrics of the shuffle read") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT key FROM testData GROUP BY key")
      val reads = collect(adaptivePlan) {
        case r: AQEShuffleReadExec => r
      }
      assert(reads.length == 1)
      val read = reads.head
      assert(!read.isLocalRead)
      assert(!read.hasSkewedPartition)
      assert(read.hasCoalescedPartition)
      assert(read.metrics.keys.toSeq.sorted == Seq(
        "numCoalescedPartitions", "numPartitions", "partitionDataSize"))
      assert(read.metrics("numCoalescedPartitions").value == 1)
      assert(read.metrics("numPartitions").value == read.partitionSpecs.length)
      assert(read.metrics("partitionDataSize").value > 0)

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
        val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM testData join testData2 ON key = a where value = '1'")
        val join = collect(adaptivePlan) {
          case j: BroadcastHashJoinExec => j
        }.head
        assert(join.buildSide == BuildLeft)

        val reads = collect(join.right) {
          case r: AQEShuffleReadExec => r
        }
        assert(reads.length == 1)
        val read = reads.head
        assert(read.isLocalRead)
        assert(read.metrics.keys.toSeq == Seq("numPartitions"))
        assert(read.metrics("numPartitions").value == read.partitionSpecs.length)
      }

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "100",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "800",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1000") {
        withTempView("skewData1", "skewData2") {
          spark
            .range(0, 1000, 1, 10)
            .select(
              when($"id" < 250, 249)
                .when($"id" >= 750, 1000)
                .otherwise($"id").as("key1"),
              $"id" as "value1")
            .createOrReplaceTempView("skewData1")
          spark
            .range(0, 1000, 1, 10)
            .select(
              when($"id" < 250, 249)
                .otherwise($"id").as("key2"),
              $"id" as "value2")
            .createOrReplaceTempView("skewData2")
          val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
            "SELECT * FROM skewData1 join skewData2 ON key1 = key2")
          val reads = collect(adaptivePlan) {
            case r: AQEShuffleReadExec => r
          }
          reads.foreach { read =>
            assert(!read.isLocalRead)
            assert(read.hasCoalescedPartition)
            assert(read.hasSkewedPartition)
            assert(read.metrics.contains("numSkewedPartitions"))
          }
          assert(reads(0).metrics("numSkewedPartitions").value == 2)
          assert(reads(0).metrics("numSkewedSplits").value == 11)
          assert(reads(1).metrics("numSkewedPartitions").value == 1)
          assert(reads(1).metrics("numSkewedSplits").value == 9)
        }
      }
    }
  }

  test("control a plan explain mode in listeners via SQLConf") {

    def checkPlanDescription(mode: String, expected: Seq[String]): Unit = {
      var checkDone = false
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case SparkListenerSQLAdaptiveExecutionUpdate(_, planDescription, _) =>
              assert(expected.forall(planDescription.contains))
              checkDone = true
            case _ => // ignore other events
          }
        }
      }
      spark.sparkContext.addSparkListener(listener)
      withSQLConf(SQLConf.UI_EXPLAIN_MODE.key -> mode,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
        val dfAdaptive = sql("SELECT * FROM testData JOIN testData2 ON key = a WHERE value = '1'")
        try {
          checkAnswer(dfAdaptive, Row(1, "1", 1, 1) :: Row(1, "1", 1, 2) :: Nil)
          spark.sparkContext.listenerBus.waitUntilEmpty()
          assert(checkDone)
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }

    Seq(("simple", Seq("== Physical Plan ==")),
        ("extended", Seq("== Parsed Logical Plan ==", "== Analyzed Logical Plan ==",
          "== Optimized Logical Plan ==", "== Physical Plan ==")),
        ("codegen", Seq("WholeStageCodegen subtrees")),
        ("cost", Seq("== Optimized Logical Plan ==", "Statistics(sizeInBytes")),
        ("formatted", Seq("== Physical Plan ==", "Output", "Arguments"))).foreach {
      case (mode, expected) =>
        checkPlanDescription(mode, expected)
    }
  }

  test("SPARK-30953: InsertAdaptiveSparkPlan should apply AQE on child plan of v2 write commands") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      var plan: SparkPlan = null
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          plan = qe.executedPlan
        }
        override def onFailure(
          funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
      }
      spark.listenerManager.register(listener)
      withTable("t1") {
        val format = classOf[NoopDataSource].getName
        Seq((0, 1)).toDF("x", "y").write.format(format).mode("overwrite").save()

        sparkContext.listenerBus.waitUntilEmpty()
        assert(plan.isInstanceOf[V2TableWriteExec])
        assert(plan.asInstanceOf[V2TableWriteExec].child.isInstanceOf[AdaptiveSparkPlanExec])

        spark.listenerManager.unregister(listener)
      }
    }
  }

  test("SPARK-37287: apply AQE on child plan of a v1 write command") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true",
        SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString) {
        withTable("t1") {
          var checkDone = false
          val listener = new SparkListener {
            override def onOtherEvent(event: SparkListenerEvent): Unit = {
              event match {
                case SparkListenerSQLAdaptiveExecutionUpdate(_, _, planInfo) =>
                  if (enabled) {
                    assert(planInfo.nodeName == "AdaptiveSparkPlan")
                    assert(planInfo.children.size == 1)
                    assert(planInfo.children.head.nodeName ==
                      "Execute InsertIntoHadoopFsRelationCommand")
                  } else {
                    assert(planInfo.nodeName == "Execute InsertIntoHadoopFsRelationCommand")
                  }
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
  }

  test("AQE should set active session during execution") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = spark.range(10).select(sum($"id"))
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
        val df = spark.range(10).groupBy($"id").count()
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
        var commands: Seq[SparkPlanInfo] = Seq.empty
        val listener = new SparkListener {
          override def onOtherEvent(event: SparkListenerEvent): Unit = {
            event match {
              case start: SparkListenerSQLExecutionStart =>
                commands = commands ++ Seq(start.sparkPlanInfo)
              case _ => // ignore other events
            }
          }
        }
        spark.sparkContext.addSparkListener(listener)
        try {
          sql("CREATE TABLE t1 USING parquet AS SELECT 1 col").collect()
          spark.sparkContext.listenerBus.waitUntilEmpty()
          assert(commands.size == 3)
          assert(commands.head.nodeName == "Execute CreateDataSourceTableAsSelectCommand")
          assert(commands(1).nodeName == "AdaptiveSparkPlan")
          assert(commands(1).children.size == 1)
          assert(commands(1).children.head.nodeName == "Execute InsertIntoHadoopFsRelationCommand")
          assert(commands(2).nodeName == "CommandResult")
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }
  }

  test("SPARK-31220, SPARK-32056: repartition by expression with AQE") {
    Seq(true, false).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "10",
        SQLConf.SHUFFLE_PARTITIONS.key -> "10") {

        val df1 = spark.range(10).repartition($"id")
        val df2 = spark.range(10).repartition($"id" + 1)

        val partitionsNum1 = df1.rdd.collectPartitions().length
        val partitionsNum2 = df2.rdd.collectPartitions().length

        if (enableAQE) {
          assert(partitionsNum1 < 10)
          assert(partitionsNum2 < 10)

          checkInitialPartitionNum(df1, 10)
          checkInitialPartitionNum(df2, 10)
        } else {
          assert(partitionsNum1 === 10)
          assert(partitionsNum2 === 10)
        }


        // Don't coalesce partitions if the number of partitions is specified.
        val df3 = spark.range(10).repartition(10, $"id")
        val df4 = spark.range(10).repartition(10)
        assert(df3.rdd.collectPartitions().length == 10)
        assert(df4.rdd.collectPartitions().length == 10)
      }
    }
  }

  test("SPARK-31220, SPARK-32056: repartition by range with AQE") {
    Seq(true, false).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "10",
        SQLConf.SHUFFLE_PARTITIONS.key -> "10") {

        val df1 = spark.range(10).toDF().repartitionByRange($"id".asc)
        val df2 = spark.range(10).toDF().repartitionByRange(($"id" + 1).asc)

        val partitionsNum1 = df1.rdd.collectPartitions().length
        val partitionsNum2 = df2.rdd.collectPartitions().length

        if (enableAQE) {
          assert(partitionsNum1 < 10)
          assert(partitionsNum2 < 10)

          checkInitialPartitionNum(df1, 10)
          checkInitialPartitionNum(df2, 10)
        } else {
          assert(partitionsNum1 === 10)
          assert(partitionsNum2 === 10)
        }

        // Don't coalesce partitions if the number of partitions is specified.
        val df3 = spark.range(10).repartitionByRange(10, $"id".asc)
        assert(df3.rdd.collectPartitions().length == 10)
      }
    }
  }

  test("SPARK-31220, SPARK-32056: repartition using sql and hint with AQE") {
    Seq(true, false).foreach { enableAQE =>
      withTempView("test") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "10",
          SQLConf.SHUFFLE_PARTITIONS.key -> "10") {

          spark.range(10).toDF().createTempView("test")

          val df1 = spark.sql("SELECT /*+ REPARTITION(id) */ * from test")
          val df2 = spark.sql("SELECT /*+ REPARTITION_BY_RANGE(id) */ * from test")
          val df3 = spark.sql("SELECT * from test DISTRIBUTE BY id")
          val df4 = spark.sql("SELECT * from test CLUSTER BY id")

          val partitionsNum1 = df1.rdd.collectPartitions().length
          val partitionsNum2 = df2.rdd.collectPartitions().length
          val partitionsNum3 = df3.rdd.collectPartitions().length
          val partitionsNum4 = df4.rdd.collectPartitions().length

          if (enableAQE) {
            assert(partitionsNum1 < 10)
            assert(partitionsNum2 < 10)
            assert(partitionsNum3 < 10)
            assert(partitionsNum4 < 10)

            checkInitialPartitionNum(df1, 10)
            checkInitialPartitionNum(df2, 10)
            checkInitialPartitionNum(df3, 10)
            checkInitialPartitionNum(df4, 10)
          } else {
            assert(partitionsNum1 === 10)
            assert(partitionsNum2 === 10)
            assert(partitionsNum3 === 10)
            assert(partitionsNum4 === 10)
          }

          // Don't coalesce partitions if the number of partitions is specified.
          val df5 = spark.sql("SELECT /*+ REPARTITION(10, id) */ * from test")
          val df6 = spark.sql("SELECT /*+ REPARTITION_BY_RANGE(10, id) */ * from test")
          assert(df5.rdd.collectPartitions().length == 10)
          assert(df6.rdd.collectPartitions().length == 10)
        }
      }
    }
  }

  test("SPARK-32573: Eliminate NAAJ when BuildSide is HashedRelationWithAllNullKeys") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString) {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData2 t1 WHERE t1.b NOT IN (SELECT b FROM testData3)")
      val bhj = findTopLevelBroadcastHashJoin(plan)
      assert(bhj.size == 1)
      val join = findTopLevelBaseJoin(adaptivePlan)
      assert(join.isEmpty)
      checkNumLocalShuffleReads(adaptivePlan)
    }
  }

  test("SPARK-32717: AQEOptimizer should respect excludedRules configuration") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
      // This test is a copy of test(SPARK-32573), in order to test the configuration
      // `spark.sql.adaptive.optimizer.excludedRules` works as expect.
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData2 t1 WHERE t1.b NOT IN (SELECT b FROM testData3)")
      val bhj = findTopLevelBroadcastHashJoin(plan)
      assert(bhj.size == 1)
      val join = findTopLevelBaseJoin(adaptivePlan)
      // this is different compares to test(SPARK-32573) due to the rule
      // `EliminateUnnecessaryJoin` has been excluded.
      assert(join.nonEmpty)
      checkNumLocalShuffleReads(adaptivePlan)
    }
  }

  test("SPARK-32649: Eliminate inner and semi join to empty relation") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      Seq(
        // inner join (small table at right side)
        "SELECT * FROM testData t1 join testData3 t2 ON t1.key = t2.a WHERE t2.b = 1",
        // inner join (small table at left side)
        "SELECT * FROM testData3 t1 join testData t2 ON t1.a = t2.key WHERE t1.b = 1",
        // left semi join
        "SELECT * FROM testData t1 left semi join testData3 t2 ON t1.key = t2.a AND t2.b = 1"
      ).foreach(query => {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        val smj = findTopLevelSortMergeJoin(plan)
        assert(smj.size == 1)
        val join = findTopLevelBaseJoin(adaptivePlan)
        assert(join.isEmpty)
        checkNumLocalShuffleReads(adaptivePlan)
      })
    }
  }

  test("SPARK-34533: Eliminate left anti join to empty relation") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      Seq(
        // broadcast non-empty right side
        ("SELECT /*+ broadcast(testData3) */ * FROM testData LEFT ANTI JOIN testData3", true),
        // broadcast empty right side
        ("SELECT /*+ broadcast(emptyTestData) */ * FROM testData LEFT ANTI JOIN emptyTestData",
          true),
        // broadcast left side
        ("SELECT /*+ broadcast(testData) */ * FROM testData LEFT ANTI JOIN testData3", false)
      ).foreach { case (query, isEliminated) =>
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        assert(findTopLevelBaseJoin(plan).size == 1)
        assert(findTopLevelBaseJoin(adaptivePlan).isEmpty == isEliminated)
      }
    }
  }

  test("SPARK-34781: Eliminate left semi/anti join to its left side") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      Seq(
        // left semi join and non-empty right side
        ("SELECT * FROM testData LEFT SEMI JOIN testData3", true),
        // left semi join, non-empty right side and non-empty join condition
        ("SELECT * FROM testData t1 LEFT SEMI JOIN testData3 t2 ON t1.key = t2.a", false),
        // left anti join and empty right side
        ("SELECT * FROM testData LEFT ANTI JOIN emptyTestData", true),
        // left anti join, empty right side and non-empty join condition
        ("SELECT * FROM testData t1 LEFT ANTI JOIN emptyTestData t2 ON t1.key = t2.key", true)
      ).foreach { case (query, isEliminated) =>
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        assert(findTopLevelBaseJoin(plan).size == 1)
        assert(findTopLevelBaseJoin(adaptivePlan).isEmpty == isEliminated)
      }
    }
  }

  test("SPARK-35455: Unify empty relation optimization between normal and AQE optimizer " +
    "- single join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      Seq(
        // left semi join and empty left side
        ("SELECT * FROM (SELECT * FROM testData WHERE value = '0')t1 LEFT SEMI JOIN " +
          "testData2 t2 ON t1.key = t2.a", true),
        // left anti join and empty left side
        ("SELECT * FROM (SELECT * FROM testData WHERE value = '0')t1 LEFT ANTI JOIN " +
          "testData2 t2 ON t1.key = t2.a", true),
        // left outer join and empty left side
        ("SELECT * FROM (SELECT * FROM testData WHERE key = 0)t1 LEFT JOIN testData2 t2 ON " +
          "t1.key = t2.a", true),
        // left outer join and non-empty left side
        ("SELECT * FROM testData t1 LEFT JOIN testData2 t2 ON " +
          "t1.key = t2.a", false),
        // right outer join and empty right side
        ("SELECT * FROM testData t1 RIGHT JOIN (SELECT * FROM testData2 WHERE b = 0)t2 ON " +
          "t1.key = t2.a", true),
        // right outer join and non-empty right side
        ("SELECT * FROM testData t1 RIGHT JOIN testData2 t2 ON " +
          "t1.key = t2.a", false),
        // full outer join and both side empty
        ("SELECT * FROM (SELECT * FROM testData WHERE key = 0)t1 FULL JOIN " +
          "(SELECT * FROM testData2 WHERE b = 0)t2 ON t1.key = t2.a", true),
        // full outer join and left side empty right side non-empty
        ("SELECT * FROM (SELECT * FROM testData WHERE key = 0)t1 FULL JOIN " +
          "testData2 t2 ON t1.key = t2.a", true)
      ).foreach { case (query, isEliminated) =>
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        assert(findTopLevelBaseJoin(plan).size == 1)
        assert(findTopLevelBaseJoin(adaptivePlan).isEmpty == isEliminated, adaptivePlan)
      }
    }
  }

  test("SPARK-35455: Unify empty relation optimization between normal and AQE optimizer " +
    "- multi join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      Seq(
        """
         |SELECT * FROM testData t1
         | JOIN (SELECT * FROM testData2 WHERE b = 0) t2 ON t1.key = t2.a
         | LEFT JOIN testData2 t3 ON t1.key = t3.a
         |""".stripMargin,
        """
         |SELECT * FROM (SELECT * FROM testData WHERE key = 0) t1
         | LEFT ANTI JOIN testData2 t2
         | FULL JOIN (SELECT * FROM testData2 WHERE b = 0) t3 ON t1.key = t3.a
         |""".stripMargin,
        """
         |SELECT * FROM testData t1
         | LEFT SEMI JOIN (SELECT * FROM testData2 WHERE b = 0)
         | RIGHT JOIN testData2 t3 on t1.key = t3.a
         |""".stripMargin
      ).foreach { query =>
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        assert(findTopLevelBaseJoin(plan).size == 2)
        assert(findTopLevelBaseJoin(adaptivePlan).isEmpty)
      }
    }
  }

  test("SPARK-35585: Support propagate empty relation through project/filter") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val (plan1, adaptivePlan1) = runAdaptiveAndVerifyResult(
        "SELECT key FROM testData WHERE key = 0 ORDER BY key, value")
      assert(findTopLevelSort(plan1).size == 1)
      assert(stripAQEPlan(adaptivePlan1).isInstanceOf[EmptyRelationExec])

      val (plan2, adaptivePlan2) = runAdaptiveAndVerifyResult(
       "SELECT key FROM (SELECT * FROM testData WHERE value = 'no_match' ORDER BY key)" +
         " WHERE key > rand()")
      assert(findTopLevelSort(plan2).size == 1)
      assert(stripAQEPlan(adaptivePlan2).isInstanceOf[EmptyRelationExec])
    }
  }

  test("SPARK-35442: Support propagate empty relation through aggregate") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (plan1, adaptivePlan1) = runAdaptiveAndVerifyResult(
        "SELECT key, count(*) FROM testData WHERE value = 'no_match' GROUP BY key")
      assert(!plan1.isInstanceOf[EmptyRelationExec])
      assert(stripAQEPlan(adaptivePlan1).isInstanceOf[EmptyRelationExec])

      val (plan2, adaptivePlan2) = runAdaptiveAndVerifyResult(
        "SELECT key, count(*) FROM testData WHERE value = 'no_match' GROUP BY key limit 1")
      assert(!plan2.isInstanceOf[EmptyRelationExec])
      assert(stripAQEPlan(adaptivePlan2).isInstanceOf[EmptyRelationExec])

      val (plan3, adaptivePlan3) = runAdaptiveAndVerifyResult(
        "SELECT count(*) FROM testData WHERE value = 'no_match'")
      assert(!plan3.isInstanceOf[EmptyRelationExec])
      assert(!stripAQEPlan(adaptivePlan3).isInstanceOf[EmptyRelationExec])
    }
  }

  test("SPARK-35442: Support propagate empty relation through union") {
    def checkNumUnion(plan: SparkPlan, numUnion: Int): Unit = {
      assert(
        collect(plan) {
          case u: UnionExec => u
        }.size == numUnion)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (plan1, adaptivePlan1) = runAdaptiveAndVerifyResult(
        """
          |SELECT key, count(*) FROM testData WHERE value = 'no_match' GROUP BY key
          |UNION ALL
          |SELECT key, 1 FROM testData
          |""".stripMargin)
      checkNumUnion(plan1, 1)
      checkNumUnion(adaptivePlan1, 0)
      assert(!stripAQEPlan(adaptivePlan1).isInstanceOf[EmptyRelationExec])

      val (plan2, adaptivePlan2) = runAdaptiveAndVerifyResult(
        """
          |SELECT key, count(*) FROM testData WHERE value = 'no_match' GROUP BY key
          |UNION ALL
          |SELECT /*+ REPARTITION */ key, 1 FROM testData WHERE value = 'no_match'
          |""".stripMargin)
      checkNumUnion(plan2, 1)
      checkNumUnion(adaptivePlan2, 0)
      assert(stripAQEPlan(adaptivePlan2).isInstanceOf[EmptyRelationExec])
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

  test("Logging plan changes for AQE") {
    val testAppender = new LogAppender("plan changes")
    withLogAppender(testAppender) {
      withSQLConf(
          SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
        sql("SELECT * FROM testData JOIN testData2 ON key = a " +
          "WHERE value = (SELECT max(a) FROM testData3)").collect()
      }
      Seq("=== Result of Batch AQE Preparations ===",
          "=== Result of Batch AQE Post Stage Creation ===",
          "=== Result of Batch AQE Replanning ===",
          "=== Result of Batch AQE Query Stage Optimization ===").foreach { expectedMsg =>
        assert(testAppender.loggingEvents.exists(
          _.getMessage.getFormattedMessage.contains(expectedMsg)))
      }
    }
  }

  test("SPARK-32932: Do not use local shuffle read at final stage on write command") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val data = for (
        i <- 1L to 10L;
        j <- 1L to 3L
      ) yield (i, j)

      val df = data.toDF("i", "j").repartition($"j")
      var noLocalread: Boolean = false
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          stripAQEPlan(qe.executedPlan) match {
            case plan @ (_: DataWritingCommandExec | _: V2TableWriteExec) =>
              noLocalread = collect(plan) {
                case exec: AQEShuffleReadExec if exec.isLocalRead => exec
              }.isEmpty
            case _ => // ignore other events
          }
        }
        override def onFailure(funcName: String, qe: QueryExecution,
          exception: Exception): Unit = {}
      }
      spark.listenerManager.register(listener)

      withTable("t") {
        df.write.partitionBy("j").saveAsTable("t")
        sparkContext.listenerBus.waitUntilEmpty()
        assert(noLocalread)
        noLocalread = false
      }

      // Test DataSource v2
      val format = classOf[NoopDataSource].getName
      df.write.format(format).mode("overwrite").save()
      sparkContext.listenerBus.waitUntilEmpty()
      assert(noLocalread)
      noLocalread = false

      spark.listenerManager.unregister(listener)
    }
  }

  test("SPARK-33494: Do not use local shuffle read for repartition") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = spark.table("testData").repartition($"key")
      df.collect()
      // local shuffle read breaks partitioning and shouldn't be used for repartition operation
      // which is specified by users.
      checkNumLocalShuffleReads(df.queryExecution.executedPlan, numShufflesWithoutLocalRead = 1)
    }
  }

  test("SPARK-33551: Do not use AQE shuffle read for repartition") {
    def hasRepartitionShuffle(plan: SparkPlan): Boolean = {
      find(plan) {
        case s: ShuffleExchangeLike =>
          s.shuffleOrigin == REPARTITION_BY_COL || s.shuffleOrigin == REPARTITION_BY_NUM
        case _ => false
      }.isDefined
    }

    def checkBHJ(
        df: Dataset[Row],
        optimizeOutRepartition: Boolean,
        probeSideLocalRead: Boolean,
        probeSideCoalescedRead: Boolean): Unit = {
      df.collect()
      val plan = df.queryExecution.executedPlan
      // There should be only one shuffle that can't do local read, which is either the top shuffle
      // from repartition, or BHJ probe side shuffle.
      checkNumLocalShuffleReads(plan, 1)
      assert(hasRepartitionShuffle(plan) == !optimizeOutRepartition)
      val bhj = findTopLevelBroadcastHashJoin(plan)
      assert(bhj.length == 1)

      // Build side should do local read.
      val buildSide = find(bhj.head.left)(_.isInstanceOf[AQEShuffleReadExec])
      assert(buildSide.isDefined)
      assert(buildSide.get.asInstanceOf[AQEShuffleReadExec].isLocalRead)

      val probeSide = find(bhj.head.right)(_.isInstanceOf[AQEShuffleReadExec])
      if (probeSideLocalRead || probeSideCoalescedRead) {
        assert(probeSide.isDefined)
        if (probeSideLocalRead) {
          assert(probeSide.get.asInstanceOf[AQEShuffleReadExec].isLocalRead)
        } else {
          assert(probeSide.get.asInstanceOf[AQEShuffleReadExec].hasCoalescedPartition)
        }
      } else {
        assert(probeSide.isEmpty)
      }
    }

    def checkSMJ(
        df: Dataset[Row],
        optimizeOutRepartition: Boolean,
        optimizeSkewJoin: Boolean,
        coalescedRead: Boolean): Unit = {
      df.collect()
      val plan = df.queryExecution.executedPlan
      assert(hasRepartitionShuffle(plan) == !optimizeOutRepartition)
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.length == 1)
      assert(smj.head.isSkewJoin == optimizeSkewJoin)
      val aqeReads = collect(smj.head) {
        case c: AQEShuffleReadExec => c
      }
      if (coalescedRead || optimizeSkewJoin) {
        assert(aqeReads.length == 2)
        if (coalescedRead) assert(aqeReads.forall(_.hasCoalescedPartition))
      } else {
        assert(aqeReads.isEmpty)
      }
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      val df = sql(
        """
          |SELECT * FROM (
          |  SELECT * FROM testData WHERE key = 1
          |)
          |RIGHT OUTER JOIN testData2
          |ON CAST(value AS INT) = b
        """.stripMargin)

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
        // Repartition with no partition num specified.
        checkBHJ(df.repartition($"b"),
          // The top shuffle from repartition is optimized out.
          optimizeOutRepartition = true, probeSideLocalRead = false, probeSideCoalescedRead = true)

        // Repartition with default partition num (5 in test env) specified.
        checkBHJ(df.repartition(5, $"b"),
          // The top shuffle from repartition is optimized out
          // The final plan must have 5 partitions, no optimization can be made to the probe side.
          optimizeOutRepartition = true, probeSideLocalRead = false, probeSideCoalescedRead = false)

        // Repartition with non-default partition num specified.
        checkBHJ(df.repartition(4, $"b"),
          // The top shuffle from repartition is not optimized out
          optimizeOutRepartition = false, probeSideLocalRead = true, probeSideCoalescedRead = true)

        // Repartition by col and project away the partition cols
        checkBHJ(df.repartition($"b").select($"key"),
          // The top shuffle from repartition is not optimized out
          optimizeOutRepartition = false, probeSideLocalRead = true, probeSideCoalescedRead = true)
      }

      // Force skew join
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "1",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR.key -> "0",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10") {
        // Repartition with no partition num specified.
        checkSMJ(df.repartition($"b"),
          // The top shuffle from repartition is optimized out.
          optimizeOutRepartition = true, optimizeSkewJoin = false, coalescedRead = true)

        // Repartition with default partition num (5 in test env) specified.
        checkSMJ(df.repartition(5, $"b"),
          // The top shuffle from repartition is optimized out.
          // The final plan must have 5 partitions, can't do coalesced read.
          optimizeOutRepartition = true, optimizeSkewJoin = false, coalescedRead = false)

        // Repartition with non-default partition num specified.
        checkSMJ(df.repartition(4, $"b"),
          // The top shuffle from repartition is not optimized out.
          optimizeOutRepartition = false, optimizeSkewJoin = true, coalescedRead = false)

        // Repartition by col and project away the partition cols
        checkSMJ(df.repartition($"b").select($"key"),
          // The top shuffle from repartition is not optimized out.
          optimizeOutRepartition = false, optimizeSkewJoin = true, coalescedRead = false)
      }
    }
  }

  test("SPARK-34091: Batch shuffle fetch in AQE partition coalescing") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10",
      SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key -> "true") {
      withTable("t1") {
        spark.range(100).selectExpr("id + 1 as a").write.format("parquet").saveAsTable("t1")
        val query = "SELECT SUM(a) FROM t1 GROUP BY a"
        val (_, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        val metricName = SQLShuffleReadMetricsReporter.LOCAL_BLOCKS_FETCHED
        val blocksFetchedMetric = collectFirst(adaptivePlan) {
          case p if p.metrics.contains(metricName) => p.metrics(metricName)
        }
        assert(blocksFetchedMetric.isDefined)
        val blocksFetched = blocksFetchedMetric.get.value
        withSQLConf(SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key -> "false") {
          val (_, adaptivePlan2) = runAdaptiveAndVerifyResult(query)
          val blocksFetchedMetric2 = collectFirst(adaptivePlan2) {
            case p if p.metrics.contains(metricName) => p.metrics(metricName)
          }
          assert(blocksFetchedMetric2.isDefined)
          val blocksFetched2 = blocksFetchedMetric2.get.value
          assert(blocksFetched < blocksFetched2)
        }
      }
    }
  }

  test("SPARK-33933: Materialize BroadcastQueryStage first in AQE") {
    val testAppender = new LogAppender("aqe query stage materialization order test")
    testAppender.setThreshold(Level.DEBUG)
    val df = spark.range(1000).select($"id" % 26, $"id" % 10)
      .toDF("index", "pv")
    val dim = Range(0, 26).map(x => (x, ('a' + x).toChar.toString))
      .toDF("index", "name")
    val testDf = df.groupBy("index")
      .agg(sum($"pv").alias("pv"))
      .join(dim, Seq("index"))
    val loggerNames =
      Seq(classOf[BroadcastQueryStageExec].getName, classOf[ShuffleQueryStageExec].getName)
    withLogAppender(testAppender, loggerNames, level = Some(Level.DEBUG)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        val result = testDf.collect()
        assert(result.length == 26)
      }
    }
    val materializeLogs = testAppender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.startsWith("Materialize query stage"))
      .toArray
    assert(materializeLogs(0).startsWith("Materialize query stage: BroadcastQueryStageExec-1"))
    assert(materializeLogs(1).startsWith("Materialize query stage: ShuffleQueryStageExec-0"))
  }

  test("SPARK-34899: Use origin plan if we can not coalesce shuffle partition") {
    def checkNoCoalescePartitions(ds: Dataset[Row], origin: ShuffleOrigin): Unit = {
      assert(collect(ds.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec if s.shuffleOrigin == origin && s.numPartitions == 2 => s
      }.size == 1)
      ds.collect()
      val plan = ds.queryExecution.executedPlan
      assert(collect(plan) {
        case c: AQEShuffleReadExec => c
      }.isEmpty)
      assert(collect(plan) {
        case s: ShuffleExchangeExec if s.shuffleOrigin == origin && s.numPartitions == 2 => s
      }.size == 1)
      checkAnswer(ds, testData)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      // Pick a small value so that no coalesce can happen.
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val df = spark.sparkContext.parallelize(
        (1 to 100).map(i => TestData(i, i.toString)), 10).toDF()

      // partition size [1420, 1420]
      checkNoCoalescePartitions(df.repartition($"key"), REPARTITION_BY_COL)
      // partition size [1140, 1119]
      checkNoCoalescePartitions(df.sort($"key"), ENSURE_REQUIREMENTS)
    }
  }

  test("SPARK-34980: Support coalesce partition through union") {
    def checkResultPartition(
        df: Dataset[Row],
        numUnion: Int,
        numShuffleReader: Int,
        numPartition: Int): Unit = {
      df.collect()
      assert(collect(df.queryExecution.executedPlan) {
        case u: UnionExec => u
      }.size == numUnion)
      assert(collect(df.queryExecution.executedPlan) {
        case r: AQEShuffleReadExec => r
      }.size === numShuffleReader)
      assert(df.rdd.partitions.length === numPartition)
    }

    Seq(true, false).foreach { combineUnionEnabled =>
      val combineUnionConfig = if (combineUnionEnabled) {
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ""
      } else {
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.CombineUnions"
      }
      // advisory partition size 1048576 has no special meaning, just a big enough value
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1048576",
          SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "10",
          combineUnionConfig) {
        withTempView("t1", "t2") {
          spark.sparkContext.parallelize((1 to 10).map(i => TestData(i, i.toString)), 2)
            .toDF().createOrReplaceTempView("t1")
          spark.sparkContext.parallelize((1 to 10).map(i => TestData(i, i.toString)), 4)
            .toDF().createOrReplaceTempView("t2")

          // positive test that could be coalesced
          checkResultPartition(
            sql("""
                |SELECT key, count(*) FROM t1 GROUP BY key
                |UNION ALL
                |SELECT * FROM t2
              """.stripMargin),
            numUnion = 1,
            numShuffleReader = 1,
            numPartition = 1 + 4)

          checkResultPartition(
            sql("""
                |SELECT key, count(*) FROM t1 GROUP BY key
                |UNION ALL
                |SELECT * FROM t2
                |UNION ALL
                |SELECT * FROM t1
              """.stripMargin),
            numUnion = if (combineUnionEnabled) 1 else 2,
            numShuffleReader = 1,
            numPartition = 1 + 4 + 2)

          checkResultPartition(
            sql("""
                |SELECT /*+ merge(t2) */ t1.key, t2.key FROM t1 JOIN t2 ON t1.key = t2.key
                |UNION ALL
                |SELECT key, count(*) FROM t2 GROUP BY key
                |UNION ALL
                |SELECT * FROM t1
              """.stripMargin),
            numUnion = if (combineUnionEnabled) 1 else 2,
            numShuffleReader = 3,
            numPartition = 1 + 1 + 2)

          // negative test
          checkResultPartition(
            sql("SELECT * FROM t1 UNION ALL SELECT * FROM t2"),
            numUnion = if (combineUnionEnabled) 1 else 1,
            numShuffleReader = 0,
            numPartition = 2 + 4
          )
        }
      }
    }
  }

  test("SPARK-35239: Coalesce shuffle partition should handle empty input RDD") {
    withTable("t") {
      withSQLConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
        spark.sql("CREATE TABLE t (c1 int) USING PARQUET")
        val (_, adaptive) = runAdaptiveAndVerifyResult("SELECT c1, count(*) FROM t GROUP BY c1")
        assert(
          collect(adaptive) {
            case c @ AQEShuffleReadExec(_, partitionSpecs) if partitionSpecs.length == 1 =>
              assert(c.hasCoalescedPartition)
              c
          }.length == 1
        )
      }
    }
  }

  test("SPARK-35264: Support AQE side broadcastJoin threshold") {
    withTempView("t1", "t2") {
      def checkJoinStrategy(shouldBroadcast: Boolean): Unit = {
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val (origin, adaptive) = runAdaptiveAndVerifyResult(
            "SELECT t1.c1, t2.c1 FROM t1 JOIN t2 ON t1.c1 = t2.c1")
          assert(findTopLevelSortMergeJoin(origin).size == 1)
          if (shouldBroadcast) {
            assert(findTopLevelBroadcastHashJoin(adaptive).size == 1)
          } else {
            assert(findTopLevelSortMergeJoin(adaptive).size == 1)
          }
        }
      }

      // t1: 1600 bytes
      // t2: 160 bytes
      spark.sparkContext.parallelize(
        (1 to 100).map(i => TestData(i, i.toString)), 10)
        .toDF("c1", "c2").createOrReplaceTempView("t1")
      spark.sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString)), 5)
        .toDF("c1", "c2").createOrReplaceTempView("t2")

      checkJoinStrategy(false)
      withSQLConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        checkJoinStrategy(false)
      }

      withSQLConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "160") {
        checkJoinStrategy(true)
      }
    }
  }

  test("SPARK-35264: Support AQE side shuffled hash join formula") {
    withTempView("t1", "t2") {
      def checkJoinStrategy(shouldShuffleHashJoin: Boolean): Unit = {
        Seq("100", "100000").foreach { size =>
          withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> size) {
            val (origin1, adaptive1) = runAdaptiveAndVerifyResult(
              "SELECT t1.c1, t2.c1 FROM t1 JOIN t2 ON t1.c1 = t2.c1")
            assert(findTopLevelSortMergeJoin(origin1).size === 1)
            if (shouldShuffleHashJoin && size.toInt < 100000) {
              val shj = findTopLevelShuffledHashJoin(adaptive1)
              assert(shj.size === 1)
              assert(shj.head.buildSide == BuildRight)
            } else {
              assert(findTopLevelSortMergeJoin(adaptive1).size === 1)
            }
          }
        }
        // respect user specified join hint
        val (origin2, adaptive2) = runAdaptiveAndVerifyResult(
          "SELECT /*+ MERGE(t1) */ t1.c1, t2.c1 FROM t1 JOIN t2 ON t1.c1 = t2.c1")
        assert(findTopLevelSortMergeJoin(origin2).size === 1)
        assert(findTopLevelSortMergeJoin(adaptive2).size === 1)
      }

      spark.sparkContext.parallelize(
        (1 to 100).map(i => TestData(i, i.toString)), 10)
        .toDF("c1", "c2").createOrReplaceTempView("t1")
      spark.sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString)), 5)
        .toDF("c1", "c2").createOrReplaceTempView("t2")

      // t1 partition size: [926, 729, 731]
      // t2 partition size: [318, 120, 0]
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
        // check default value
        checkJoinStrategy(false)
        withSQLConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "400") {
          checkJoinStrategy(true)
        }
        withSQLConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "300") {
          checkJoinStrategy(false)
        }
        withSQLConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "1000") {
          checkJoinStrategy(true)
        }
      }
    }
  }

  test("SPARK-35650: Coalesce number of partitions by AEQ") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
      Seq("REPARTITION", "REBALANCE(key)")
        .foreach {repartition =>
          val query = s"SELECT /*+ $repartition */ * FROM testData"
          val (_, adaptivePlan) = runAdaptiveAndVerifyResult(query)
          collect(adaptivePlan) {
            case r: AQEShuffleReadExec => r
          } match {
            case Seq(aqeShuffleRead) =>
              assert(aqeShuffleRead.partitionSpecs.size === 1)
              assert(!aqeShuffleRead.isLocalRead)
            case _ =>
              fail("There should be a AQEShuffleReadExec")
          }
        }
    }
  }

  test("SPARK-35650: Use local shuffle read if can not coalesce number of partitions") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val query = "SELECT /*+ REPARTITION */ * FROM testData"
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult(query)
      collect(adaptivePlan) {
        case r: AQEShuffleReadExec => r
      } match {
        case Seq(aqeShuffleRead) =>
          assert(aqeShuffleRead.partitionSpecs.size === 4)
          assert(aqeShuffleRead.isLocalRead)
        case _ =>
          fail("There should be a AQEShuffleReadExec")
      }
    }
  }

  test("SPARK-35725: Support optimize skewed partitions in RebalancePartitions") {
    withTempView("v") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {

        spark.sparkContext.parallelize(
          (1 to 10).map(i => TestData(if (i > 4) 5 else i, i.toString)), 3)
          .toDF("c1", "c2").createOrReplaceTempView("v")

        def checkPartitionNumber(
            query: String, skewedPartitionNumber: Int, totalNumber: Int): Unit = {
          val (_, adaptive) = runAdaptiveAndVerifyResult(query)
          val read = collect(adaptive) {
            case read: AQEShuffleReadExec => read
          }
          assert(read.size == 1)
          assert(read.head.partitionSpecs.count(_.isInstanceOf[PartialReducerPartitionSpec]) ==
            skewedPartitionNumber)
          assert(read.head.partitionSpecs.size == totalNumber)
        }

        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "150") {
          // partition size [0,258,72,72,72]
          checkPartitionNumber("SELECT /*+ REBALANCE(c1) */ * FROM v", 2, 4)
          // partition size [144,72,144,72,72,144,72]
          checkPartitionNumber("SELECT /*+ REBALANCE */ * FROM v", 6, 7)
        }

        // no skewed partition should be optimized
        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10000") {
          checkPartitionNumber("SELECT /*+ REBALANCE(c1) */ * FROM v", 0, 1)
        }
      }
    }
  }

  test("SPARK-35888: join with a 0-partition table") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      withTempView("t2") {
        // create a temp view with 0 partition
        spark.createDataFrame(sparkContext.emptyRDD[Row], new StructType().add("b", IntegerType))
          .createOrReplaceTempView("t2")
        val (_, adaptive) =
          runAdaptiveAndVerifyResult("SELECT * FROM testData2 t1 left semi join t2 ON t1.a=t2.b")
        val aqeReads = collect(adaptive) {
          case c: AQEShuffleReadExec => c
        }
        assert(aqeReads.length == 2)
        aqeReads.foreach { c =>
          val stats = c.child.asInstanceOf[QueryStageExec].getRuntimeStatistics
          assert(stats.sizeInBytes >= 0)
          assert(stats.rowCount.get >= 0)
        }
      }
    }
  }

  test("SPARK-33832: Support optimize skew join even if introduce extra shuffle") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10",
      SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> "true") {
      withTempView("skewData1", "skewData2") {
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 3 as key1", "id as value1")
          .createOrReplaceTempView("skewData1")
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 1 as key2", "id as value2")
          .createOrReplaceTempView("skewData2")

        // check if optimized skewed join does not satisfy the required distribution
        Seq(true, false).foreach { hasRequiredDistribution =>
          Seq(true, false).foreach { hasPartitionNumber =>
            val repartition = if (hasRequiredDistribution) {
              s"/*+ repartition(${ if (hasPartitionNumber) "10," else ""}key1) */"
            } else {
              ""
            }

            // check required distribution and extra shuffle
            val (_, adaptive1) =
              runAdaptiveAndVerifyResult(s"SELECT $repartition key1 FROM skewData1 " +
                s"JOIN skewData2 ON key1 = key2 GROUP BY key1")
            val shuffles1 = collect(adaptive1) {
              case s: ShuffleExchangeExec => s
            }
            assert(shuffles1.size == 3)
            // shuffles1.head is the top-level shuffle under the Aggregate operator
            assert(shuffles1.head.shuffleOrigin == ENSURE_REQUIREMENTS)
            val smj1 = findTopLevelSortMergeJoin(adaptive1)
            assert(smj1.size == 1 && smj1.head.isSkewJoin)

            // only check required distribution
            val (_, adaptive2) =
              runAdaptiveAndVerifyResult(s"SELECT $repartition key1 FROM skewData1 " +
                s"JOIN skewData2 ON key1 = key2")
            val shuffles2 = collect(adaptive2) {
              case s: ShuffleExchangeExec => s
            }
            if (hasRequiredDistribution) {
              assert(shuffles2.size == 3)
              val finalShuffle = shuffles2.head
              if (hasPartitionNumber) {
                assert(finalShuffle.shuffleOrigin == REPARTITION_BY_NUM)
              } else {
                assert(finalShuffle.shuffleOrigin == REPARTITION_BY_COL)
              }
            } else {
              assert(shuffles2.size == 2)
            }
            val smj2 = findTopLevelSortMergeJoin(adaptive2)
            assert(smj2.size == 1 && smj2.head.isSkewJoin)
          }
        }
      }
    }
  }

  test("SPARK-35968: AQE coalescing should not produce too small partitions by default") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (_, adaptive) =
        runAdaptiveAndVerifyResult("SELECT sum(id) FROM RANGE(10) GROUP BY id % 3")
      val coalesceRead = collect(adaptive) {
        case r: AQEShuffleReadExec if r.hasCoalescedPartition => r
      }
      assert(coalesceRead.length == 1)
      // RANGE(10) is a very small dataset and AQE coalescing should produce one partition.
      assert(coalesceRead.head.partitionSpecs.length == 1)
    }
  }

  test("SPARK-35794: Allow custom plugin for cost evaluator") {
    CostEvaluator.instantiate(
      classOf[SimpleShuffleSortCostEvaluator].getCanonicalName, spark.sparkContext.getConf)
    intercept[IllegalArgumentException] {
      CostEvaluator.instantiate(
        classOf[InvalidCostEvaluator].getCanonicalName, spark.sparkContext.getConf)
    }

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val query = "SELECT * FROM testData join testData2 ON key = a where value = '1'"

      withSQLConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key ->
        "org.apache.spark.sql.execution.adaptive.SimpleShuffleSortCostEvaluator") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        val smj = findTopLevelSortMergeJoin(plan)
        assert(smj.size == 1)
        val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
        assert(bhj.size == 1)
        checkNumLocalShuffleReads(adaptivePlan)
      }

      withSQLConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key ->
        "org.apache.spark.sql.execution.adaptive.InvalidCostEvaluator") {
        intercept[IllegalArgumentException] {
          runAdaptiveAndVerifyResult(query)
        }
      }
    }
  }

  test("SPARK-36020: Check logical link in remove redundant projects") {
    withTempView("t") {
      spark.range(10).selectExpr("id % 10 as key", "cast(id * 2 as int) as a",
        "cast(id * 3 as int) as b", "array(id, id + 1, id + 3) as c").createOrReplaceTempView("t")
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "800") {
        val query =
          """
            |WITH tt AS (
            | SELECT key, a, b, explode(c) AS c FROM t
            |)
            |SELECT t1.key, t1.c, t2.key, t2.c
            |FROM (SELECT a, b, c, key FROM tt WHERE a > 1) t1
            |JOIN (SELECT a, b, c, key FROM tt) t2
            |  ON t1.key = t2.key
            |""".stripMargin
        val (origin, adaptive) = runAdaptiveAndVerifyResult(query)
        assert(findTopLevelSortMergeJoin(origin).size == 1)
        assert(findTopLevelBroadcastHashJoin(adaptive).size == 1)
      }
    }
  }

  test("SPARK-35874: AQE Shuffle should wait for its subqueries to finish before materializing") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val query = "SELECT b FROM testData2 DISTRIBUTE BY (b, (SELECT max(key) FROM testData))"
      runAdaptiveAndVerifyResult(query)
    }
  }

  test("SPARK-36032: Use inputPlan instead of currentPhysicalPlan to initialize logical link") {
    withTempView("v") {
      spark.sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString)), 2)
        .toDF("c1", "c2").createOrReplaceTempView("v")

      Seq("-1", "10000").foreach { aqeBhj =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> aqeBhj,
          SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          val (origin, adaptive) = runAdaptiveAndVerifyResult(
            """
              |SELECT * FROM v t1 JOIN (
              | SELECT c1 + 1 as c3 FROM v
              |)t2 ON t1.c1 = t2.c3
              |SORT BY c1
          """.stripMargin)
          if (aqeBhj.toInt < 0) {
            // 1 sort since spark plan has no shuffle for SMJ
            assert(findTopLevelSort(origin).size == 1)
            // 2 sorts in SMJ
            assert(findTopLevelSort(adaptive).size == 2)
          } else {
            assert(findTopLevelSort(origin).size == 1)
            // 1 sort at top node and BHJ has no sort
            assert(findTopLevelSort(adaptive).size == 1)
          }
        }
      }
    }
  }

  test("SPARK-36424: Support eliminate limits in AQE Optimizer") {
    withTempView("v") {
      spark.sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, if (i > 2) "2" else i.toString)), 2)
        .toDF("c1", "c2").createOrReplaceTempView("v")

      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "3") {
        val (origin1, adaptive1) = runAdaptiveAndVerifyResult(
          """
            |SELECT c2, sum(c1) FROM v GROUP BY c2 LIMIT 5
          """.stripMargin)
        assert(findTopLevelLimit(origin1).size == 1)
        assert(findTopLevelLimit(adaptive1).isEmpty)

        // eliminate limit through filter
        val (origin2, adaptive2) = runAdaptiveAndVerifyResult(
          """
            |SELECT c2, sum(c1) FROM v GROUP BY c2 HAVING sum(c1) > 1 LIMIT 5
          """.stripMargin)
        assert(findTopLevelLimit(origin2).size == 1)
        assert(findTopLevelLimit(adaptive2).isEmpty)

        // The strategy of Eliminate Limits batch should be fixedPoint
        val (origin3, adaptive3) = runAdaptiveAndVerifyResult(
          """
            |SELECT * FROM (SELECT c1 + c2 FROM (SELECT DISTINCT * FROM v LIMIT 10086)) LIMIT 20
          """.stripMargin
        )
        assert(findTopLevelLimit(origin3).size == 1)
        assert(findTopLevelLimit(adaptive3).isEmpty)
      }
    }
  }

  test("SPARK-48037: Fix SortShuffleWriter lacks shuffle write related metrics " +
    "resulting in potentially inaccurate data") {
    withTable("t3") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> (SortShuffleManager
          .MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE + 1).toString) {
        sql("CREATE TABLE t3 USING PARQUET AS SELECT id FROM range(2)")
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          """
            |SELECT id, count(*)
            |FROM t3
            |GROUP BY id
            |LIMIT 1
            |""".stripMargin, skipCheckAnswer = true)
        // The shuffle stage produces two rows and the limit operator should not been optimized out.
        assert(findTopLevelLimit(plan).size == 1)
        assert(findTopLevelLimit(adaptivePlan).size == 1)
      }
    }
  }

  test("SPARK-37063: OptimizeSkewInRebalancePartitions support optimize non-root node") {
    withTempView("v") {
      withSQLConf(
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
        spark.sparkContext.parallelize(
          (1 to 10).map(i => TestData(if (i > 2) 2 else i, i.toString)), 2)
          .toDF("c1", "c2").createOrReplaceTempView("v")

        def checkRebalance(query: String, numShufflePartitions: Int): Unit = {
          val (_, adaptive) = runAdaptiveAndVerifyResult(query)
          assert(adaptive.collect {
            case sort: SortExec => sort
          }.size == 1)
          val read = collect(adaptive) {
            case read: AQEShuffleReadExec => read
          }
          assert(read.size == 1)
          assert(read.head.partitionSpecs.forall(_.isInstanceOf[PartialReducerPartitionSpec]))
          assert(read.head.partitionSpecs.size == numShufflePartitions)
        }

        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "50") {
          checkRebalance("SELECT /*+ REBALANCE(c1) */ * FROM v SORT BY c1", 2)
          checkRebalance("SELECT /*+ REBALANCE */ * FROM v SORT BY c1", 2)
        }
      }
    }
  }

  test("SPARK-37357: Add small partition factor for rebalance partitions") {
    withTempView("v") {
      withSQLConf(
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        spark.sparkContext.parallelize(
          (1 to 8).map(i => TestData(if (i > 2) 2 else i, i.toString)), 3)
          .toDF("c1", "c2").createOrReplaceTempView("v")

        def checkAQEShuffleReadExists(query: String, exists: Boolean): Unit = {
          val (_, adaptive) = runAdaptiveAndVerifyResult(query)
          assert(
            collect(adaptive) {
              case read: AQEShuffleReadExec => read
            }.nonEmpty == exists)
        }

        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "200") {
          withSQLConf(SQLConf.ADAPTIVE_REBALANCE_PARTITIONS_SMALL_PARTITION_FACTOR.key -> "0.5") {
            // block size: [88, 97, 97]
            checkAQEShuffleReadExists("SELECT /*+ REBALANCE(c1) */ * FROM v", false)
          }
          withSQLConf(SQLConf.ADAPTIVE_REBALANCE_PARTITIONS_SMALL_PARTITION_FACTOR.key -> "0.2") {
            // block size: [88, 97, 97]
            checkAQEShuffleReadExists("SELECT /*+ REBALANCE(c1) */ * FROM v", true)
          }
        }
      }
    }
  }

  test("SPARK-37742: AQE reads invalid InMemoryRelation stats and mistakenly plans BHJ") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1048584",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      // Spark estimates a string column as 20 bytes so with 60k rows, these relations should be
      // estimated at ~120m bytes which is greater than the broadcast join threshold.
      val joinKeyOne = "00112233445566778899"
      val joinKeyTwo = "11223344556677889900"
      Seq.fill(60000)(joinKeyOne).toDF("key")
        .createOrReplaceTempView("temp")
      Seq.fill(60000)(joinKeyTwo).toDF("key")
        .createOrReplaceTempView("temp2")

      Seq(joinKeyOne).toDF("key").createOrReplaceTempView("smallTemp")
      spark.sql("SELECT key as newKey FROM temp").persist()

      // This query is trying to set up a situation where there are three joins.
      // The first join will join the cached relation with a smaller relation.
      // The first join is expected to be a broadcast join since the smaller relation will
      // fit under the broadcast join threshold.
      // The second join will join the first join with another relation and is expected
      // to remain as a sort-merge join.
      // The third join will join the cached relation with another relation and is expected
      // to remain as a sort-merge join.
      val query =
      s"""
         |SELECT t3.newKey
         |FROM
         |  (SELECT t1.newKey
         |  FROM (SELECT key as newKey FROM temp) as t1
         |        JOIN
         |        (SELECT key FROM smallTemp) as t2
         |        ON t1.newKey = t2.key
         |  ) as t3
         |  JOIN
         |  (SELECT key FROM temp2) as t4
         |  ON t3.newKey = t4.key
         |UNION
         |SELECT t1.newKey
         |FROM
         |    (SELECT key as newKey FROM temp) as t1
         |    JOIN
         |    (SELECT key FROM temp2) as t2
         |    ON t1.newKey = t2.key
         |""".stripMargin
      val df = spark.sql(query)
      df.collect()
      val adaptivePlan = df.queryExecution.executedPlan
      val bhj = findTopLevelBroadcastHashJoin(adaptivePlan)
      assert(bhj.length == 1)
    }
  }

  test("SPARK-37328: skew join with 3 tables") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      withTempView("skewData1", "skewData2", "skewData3") {
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 3 as key1", "id % 3 as value1")
          .createOrReplaceTempView("skewData1")
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 1 as key2", "id as value2")
          .createOrReplaceTempView("skewData2")
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 1 as key3", "id as value3")
          .createOrReplaceTempView("skewData3")

        // skewedJoin doesn't happen in last stage
        val (_, adaptive1) =
          runAdaptiveAndVerifyResult("SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 " +
            "JOIN skewData3 ON value2 = value3")
        val shuffles1 = collect(adaptive1) {
          case s: ShuffleExchangeExec => s
        }
        assert(shuffles1.size == 4)
        val smj1 = findTopLevelSortMergeJoin(adaptive1)
        assert(smj1.size == 2 && smj1.last.isSkewJoin && !smj1.head.isSkewJoin)

        // Query has two skewJoin in two continuous stages.
        val (_, adaptive2) =
          runAdaptiveAndVerifyResult("SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 " +
            "JOIN skewData3 ON value1 = value3")
        val shuffles2 = collect(adaptive2) {
          case s: ShuffleExchangeExec => s
        }
        assert(shuffles2.size == 4)
        val smj2 = findTopLevelSortMergeJoin(adaptive2)
        assert(smj2.size == 2 && smj2.forall(_.isSkewJoin))
      }
    }
  }

  test("SPARK-37652: optimize skewed join through union") {
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

        def checkSkewJoin(query: String, joinNums: Int, optimizeSkewJoinNums: Int): Unit = {
          val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(query)
          val joins = findTopLevelSortMergeJoin(innerAdaptivePlan)
          val optimizeSkewJoins = joins.filter(_.isSkewJoin)
          assert(joins.size == joinNums && optimizeSkewJoins.size == optimizeSkewJoinNums)
        }

        // skewJoin union skewJoin
        checkSkewJoin(
          "SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 " +
            "UNION ALL SELECT key2 FROM skewData1 JOIN skewData2 ON key1 = key2", 2, 2)

        // skewJoin union aggregate
        checkSkewJoin(
          "SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 " +
            "UNION ALL SELECT key2 FROM skewData2 GROUP BY key2", 1, 1)

        // skewJoin1 union (skewJoin2 join aggregate)
        // skewJoin2 will lead to extra shuffles, but skew1 cannot be optimized
         checkSkewJoin(
          "SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 UNION ALL " +
            "SELECT key1 from (SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2) tmp1 " +
            "JOIN (SELECT key2 FROM skewData2 GROUP BY key2) tmp2 ON key1 = key2", 3, 0)
      }
    }
  }

  test("SPARK-38162: Optimize one row plan in AQE Optimizer") {
    withTempView("v") {
      spark.sparkContext.parallelize(
        (1 to 4).map(i => TestData(i, i.toString)), 2)
        .toDF("c1", "c2").createOrReplaceTempView("v")

      // remove sort
      val (origin1, adaptive1) = runAdaptiveAndVerifyResult(
        """
          |SELECT * FROM v where c1 = 1 order by c1, c2
          |""".stripMargin)
      assert(findTopLevelSort(origin1).size == 1)
      assert(findTopLevelSort(adaptive1).isEmpty)

      // convert group only aggregate to project
      val (origin2, adaptive2) = runAdaptiveAndVerifyResult(
        """
          |SELECT distinct c1 FROM (SELECT /*+ repartition(c1) */ * FROM v where c1 = 1)
          |""".stripMargin)
      assert(findTopLevelAggregate(origin2).size == 2)
      assert(findTopLevelAggregate(adaptive2).isEmpty)

      // remove distinct in aggregate
      val (origin3, adaptive3) = runAdaptiveAndVerifyResult(
        """
          |SELECT sum(distinct c1) FROM (SELECT /*+ repartition(c1) */ * FROM v where c1 = 1)
          |""".stripMargin)
      assert(findTopLevelAggregate(origin3).size == 4)
      assert(findTopLevelAggregate(adaptive3).size == 2)

      // do not optimize if the aggregate is inside query stage
      val (origin4, adaptive4) = runAdaptiveAndVerifyResult(
        """
          |SELECT distinct c1 FROM v where c1 = 1
          |""".stripMargin)
      assert(findTopLevelAggregate(origin4).size == 2)
      assert(findTopLevelAggregate(adaptive4).size == 2)

      val (origin5, adaptive5) = runAdaptiveAndVerifyResult(
        """
          |SELECT sum(distinct c1) FROM v where c1 = 1
          |""".stripMargin)
      assert(findTopLevelAggregate(origin5).size == 4)
      assert(findTopLevelAggregate(adaptive5).size == 4)
    }
  }

  test("SPARK-39551: Invalid plan check - invalid broadcast query stage") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |SELECT /*+ BROADCAST(t3) */ t3.b, count(t3.a) FROM testData2 t1
          |INNER JOIN testData2 t2
          |ON t1.b = t2.b AND t1.a = 0
          |RIGHT OUTER JOIN testData2 t3
          |ON t1.a > t3.a
          |GROUP BY t3.b
        """.stripMargin
      )
      assert(findTopLevelBroadcastNestedLoopJoin(adaptivePlan).size == 1)
    }
  }

  test("SPARK-48155: AQEPropagateEmptyRelation check remained child for join") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      // Before SPARK-48155, since the AQE will call ValidateSparkPlan,
      // all AQE optimize rule won't work and return the origin plan.
      // After SPARK-48155, Spark avoid invalid propagate of empty relation.
      // Then the UNION first child empty relation can be propagate correctly
      // and the JOIN won't be propagated since will generated a invalid plan.
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |SELECT /*+ BROADCAST(t3) */ t3.b, count(t3.a) FROM testData2 t1
          |INNER JOIN (
          |  SELECT * FROM testData2
          |  WHERE b = 0
          |  UNION ALL
          |  SELECT * FROM testData2
          |  WHErE b != 0
          |) t2
          |ON t1.b = t2.b AND t1.a = 0
          |RIGHT OUTER JOIN testData2 t3
          |ON t1.a > t3.a
          |GROUP BY t3.b
        """.stripMargin
      )
      assert(findTopLevelBroadcastNestedLoopJoin(adaptivePlan).size == 1)
      assert(findTopLevelUnion(adaptivePlan).size == 0)
    }
  }

  test("SPARK-39915: Dataset.repartition(N) may not create N partitions") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "6") {
      // partitioning:  HashPartitioning
      // shuffleOrigin: REPARTITION_BY_NUM
      assert(spark.range(0).repartition(5, $"id").rdd.getNumPartitions == 5)
      // shuffleOrigin: REPARTITION_BY_COL
      // The minimum partition number after AQE coalesce is 1
      assert(spark.range(0).repartition($"id").rdd.getNumPartitions == 1)
      // through project
      assert(spark.range(0).selectExpr("id % 3 as c1", "id % 7 as c2")
        .repartition(5, $"c1").select($"c2").rdd.getNumPartitions == 5)

      // partitioning:  RangePartitioning
      // shuffleOrigin: REPARTITION_BY_NUM
      // The minimum partition number of RangePartitioner is 1
      assert(spark.range(0).repartitionByRange(5, $"id").rdd.getNumPartitions == 1)
      // shuffleOrigin: REPARTITION_BY_COL
      assert(spark.range(0).repartitionByRange($"id").rdd.getNumPartitions == 1)

      // partitioning:  RoundRobinPartitioning
      // shuffleOrigin: REPARTITION_BY_NUM
      assert(spark.range(0).repartition(5).rdd.getNumPartitions == 5)
      // shuffleOrigin: REBALANCE_PARTITIONS_BY_NONE
      assert(spark.range(0).repartition().rdd.getNumPartitions == 1)
      // through project
      assert(spark.range(0).selectExpr("id % 3 as c1", "id % 7 as c2")
        .repartition(5).select($"c2").rdd.getNumPartitions == 5)

      // partitioning:  SinglePartition
      assert(spark.range(0).repartition(1).rdd.getNumPartitions == 1)
    }
  }

  test("SPARK-39915: Ensure the output partitioning is user-specified") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(1).selectExpr("id as c1")
      val df2 = spark.range(1).selectExpr("id as c2")
      val df = df1.join(df2, col("c1") === col("c2")).repartition(3, col("c1"))
      assert(df.rdd.getNumPartitions == 3)
    }
  }

  test("SPARK-42778: QueryStageExec should respect supportsRowBased") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      withTempView("t") {
        Seq(1).toDF("c1").createOrReplaceTempView("t")
        spark.catalog.cacheTable("t")
        val df = spark.table("t")
        df.collect()
        assert(collect(df.queryExecution.executedPlan) {
          case c: ColumnarToRowExec => c
        }.isEmpty)
      }
    }
  }

  test("SPARK-42101: Apply AQE if contains nested AdaptiveSparkPlanExec") {
    withSQLConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true") {
      val df = spark.range(3).repartition().cache()
      assert(df.sortWithinPartitions("id")
        .queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
    }
  }

  test("SPARK-42101: Make AQE support InMemoryTableScanExec") {
    withSQLConf(
        SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(10).selectExpr("cast(id as string) c1")
      val df2 = spark.range(10).selectExpr("cast(id as string) c2")
      val cached = df1.join(df2, $"c1" === $"c2").cache()

      def checkShuffleAndSort(firstAccess: Boolean): Unit = {
        val df = cached.groupBy("c1").agg(max($"c2"))
        val initialExecutedPlan = df.queryExecution.executedPlan
        assert(collect(initialExecutedPlan) {
          case s: ShuffleExchangeLike => s
        }.size == (if (firstAccess) 1 else 0))
        assert(collect(initialExecutedPlan) {
          case s: SortExec => s
        }.size == (if (firstAccess) 2 else 0))
        assert(collect(initialExecutedPlan) {
          case i: InMemoryTableScanLike => i
        }.head.isMaterialized != firstAccess)

        df.collect()
        val finalExecutedPlan = df.queryExecution.executedPlan
        assert(collect(finalExecutedPlan) {
          case s: ShuffleExchangeLike => s
        }.isEmpty)
        assert(collect(finalExecutedPlan) {
          case s: SortExec => s
        }.isEmpty)
        assert(collect(initialExecutedPlan) {
          case i: InMemoryTableScanLike => i
        }.head.isMaterialized)
      }

      // first access cache
      checkShuffleAndSort(firstAccess = true)

      // access a materialized cache
      checkShuffleAndSort(firstAccess = false)
    }
  }

  test("SPARK-42101: Do not coalesce shuffle partition if other side is TableCacheQueryStage") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
      withTempView("v1", "v2") {
        Seq(1, 2).toDF("c1").repartition(3, $"c1").cache().createOrReplaceTempView("v1")
        Seq(1, 2).toDF("c2").createOrReplaceTempView("v2")

        val df = spark.sql("SELECT * FROM v1 JOIN v2 ON v1.c1 = v2.c2")
        df.collect()
        val finalPlan = df.queryExecution.executedPlan
        assert(collect(finalPlan) {
          case q: ShuffleQueryStageExec => q
        }.size == 1)
        assert(collect(finalPlan) {
          case r: AQEShuffleReadExec => r
        }.isEmpty)
      }
    }
  }

  test("SPARK-42101: Coalesce shuffle partition with union even if exists TableCacheQueryStage") {
    withSQLConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
      val cached = Seq(1).toDF("c").cache()
      val df = Seq(2).toDF("c").repartition($"c").unionAll(cached)
      df.collect()
      assert(collect(df.queryExecution.executedPlan) {
        case r @ AQEShuffleReadExec(_: ShuffleQueryStageExec, _) => r
      }.size == 1)
      assert(collect(df.queryExecution.executedPlan) {
        case c: TableCacheQueryStageExec => c
      }.size == 1)
    }
  }

  test("SPARK-43026: Apply AQE with non-exchange table cache") {
    Seq(true, false).foreach { canChangeOP =>
      withSQLConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> canChangeOP.toString) {
        // No exchange, no need for AQE
        val df = spark.range(0).cache()
        df.collect()
        assert(!df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        // Has exchange, apply AQE
        val df2 = spark.range(0).repartition(1).cache()
        df2.collect()
        assert(df2.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      }
    }
  }

  test("SPARK-43376: Improve reuse subquery with table cache") {
    withSQLConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true") {
      withTable("t1", "t2") {
        withCache("t1") {
          Seq(1).toDF("c1").cache().createOrReplaceTempView("t1")
          Seq(2).toDF("c2").createOrReplaceTempView("t2")

          val (_, adaptive) = runAdaptiveAndVerifyResult(
            "SELECT * FROM t1 WHERE c1 < (SELECT c2 FROM t2)")
          assert(findReusedSubquery(adaptive).size == 1)
        }
      }
    }
  }

  test("SPARK-44040: Fix compute stats when AggregateExec nodes above QueryStageExec") {
    val emptyDf = spark.range(1).where("false")
    val aggDf1 = emptyDf.agg(sum("id").as("id")).withColumn("name", lit("df1"))
    val aggDf2 = emptyDf.agg(sum("id").as("id")).withColumn("name", lit("df2"))
    val unionDF = aggDf1.union(aggDf2)
    checkAnswer(unionDF.select("id").distinct(), Seq(Row(null)))
  }

  test("SPARK-47247: coalesce differently for BNLJ") {
    Seq(true, false).foreach { expectCoalesce =>
      val minPartitionSize = if (expectCoalesce) "64MB" else "1B"
      withSQLConf(
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> minPartitionSize) {
        val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT /*+ broadcast(testData2) */ * " +
            "FROM (SELECT value v, max(key) k from testData group by value) " +
            "JOIN testData2 ON k + a > 0")
        val bnlj = findTopLevelBroadcastNestedLoopJoin(adaptivePlan)
        assert(bnlj.size == 1)
        val coalescedReads = collect(adaptivePlan) {
          case read: AQEShuffleReadExec if read.isCoalescedRead => read
        }
        assert(coalescedReads.nonEmpty == expectCoalesce)
      }
    }
  }
}

/**
 * Invalid implementation class for [[CostEvaluator]].
 */
private class InvalidCostEvaluator() {}

/**
 * A simple [[CostEvaluator]] to count number of [[ShuffleExchangeLike]] and [[SortExec]].
 */
private case class SimpleShuffleSortCostEvaluator() extends CostEvaluator {
  override def evaluateCost(plan: SparkPlan): Cost = {
    val cost = plan.collect {
      case s: ShuffleExchangeLike => s
      case s: SortExec => s
    }.size
    SimpleCost(cost)
  }
}

/**
 * Helps to simulate ExchangeQueryStageExec materialization failure.
 */
private object TestProblematicCoalesceStrategy extends Strategy {
  private case class TestProblematicCoalesceExec(numPartitions: Int, child: SparkPlan)
    extends UnaryExecNode {
    override protected def doExecute(): RDD[InternalRow] = {
      child.execute().mapPartitions { _ =>
        throw new RuntimeException("coalesce test error")
      }
    }
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): TestProblematicCoalesceExec =
      copy(child = newChild)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case org.apache.spark.sql.catalyst.plans.logical.Repartition(
      numPartitions, false, child) =>
        TestProblematicCoalesceExec(numPartitions, planLater(child)) :: Nil
      case _ => Nil
    }
  }
}
