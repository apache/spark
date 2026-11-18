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

package org.apache.spark.sql.execution.dynamicpruning

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, DynamicPruningSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic
import org.apache.spark.sql.execution.{BaseSubqueryExec, ColumnarRule, FileSourceScanExec, InSubqueryExec, ReusedSubqueryExec, SparkPlan, SparkStrategy, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DynamicPruningPlanningSuite
    extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private val activeMeasurements = new AtomicReference[PreparationMeasurements]
  private val depths = Seq(1, 3, 5)
  private val depthFacts = (1 to depths.max).map(i => s"dpp_depth_fact_$i")
  private val fixtureTables = Seq("dpp_depth_dim") ++ depthFacts ++ Seq("dpp_dim", "dpp_b", "dpp_c")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Seq((0L, 0L, 1), (1L, 1L, 0), (2L, 2L, 0)).toDF("k", "x", "flag")
      .coalesce(1).write.saveAsTable("dpp_depth_dim")
    for ((table, index) <- depthFacts.zipWithIndex) {
      // Growing inputs keep independent DPP beneficial at every level. The second join
      // key limits the result to one row, without statically restricting the partition key.
      spark.range(300L * (1L << (2 * index)))
        .selectExpr("id % 3 AS k", "id AS x", "id + 1 AS payload")
        .coalesce(1).write.partitionBy("k").saveAsTable(table)
    }
    Seq((0L, 1), (1L, 0), (2L, 0)).toDF("k", "flag")
      .coalesce(1).write.saveAsTable("dpp_dim")
    for ((table, size) <- Seq("dpp_b" -> 30L, "dpp_c" -> 3000L)) {
      spark.range(size).selectExpr("id % 3 AS k", "id AS payload")
        .coalesce(1).write.partitionBy("k").saveAsTable(table)
    }
  }

  override protected def afterAll(): Unit = {
    try {
      fixtureTables.foreach(table => sql(s"DROP TABLE IF EXISTS $table"))
    } finally {
      super.afterAll()
    }
  }

  override protected def createSparkSession: classic.SparkSession = {
    val session = super.createSparkSession
    // Observe existing preparation checkpoints without copying or wrapping any physical node.
    session.extensions.injectColumnar(_ => new ColumnarRule {
      override def preColumnarTransitions: Rule[SparkPlan] = observePreparation(adaptive = false)
    })
    session.extensions.injectQueryStagePrepRule(_ => observePreparation(adaptive = true))
    session
  }

  private def observePreparation(adaptive: Boolean): Rule[SparkPlan] = new Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      val measurements = activeMeasurements.get()
      if (measurements != null) {
        val counter = if (adaptive) measurements.adaptive else measurements.columnar
        counter.incrementAndGet()
      }
      plan
    }
  }

  for (adaptive <- Seq(false, true); broadcastOnly <- Seq(true, false)) {
    test(s"SPARK-48486: execute DPP depth staircase, " +
        s"adaptive=$adaptive, broadcastOnly=$broadcastOnly") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> broadcastOnly.toString,
          SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false",
          SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.0",
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
          SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true",
          SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        for (depth <- depths) {
          val hint = if (broadcastOnly) "/*+ BROADCAST(q) */" else ""
          val query = (1 to depth).foldLeft(
              "SELECT k, x, CAST(0 AS BIGINT) payload FROM dpp_depth_dim WHERE flag = 1") {
            (child, level) =>
              s"SELECT $hint q.k, q.x, q.payload + f.payload payload " +
                s"FROM ($child) q JOIN dpp_depth_fact_$level f ON q.k = f.k AND q.x = f.x"
          }
          val df = sql(s"SELECT sum(payload) FROM ($query) q")
          val logicalDpp = df.queryExecution.optimizedPlan.flatMap { node =>
            node.expressions.flatMap(_.collect { case d: DynamicPruningSubquery => d })
          }
          assert(logicalDpp.size == depth)

          val measurements = new PreparationMeasurements
          val planner = new PlanningCounter
          val originalStrategies = spark.experimental.extraStrategies
          // Exclude fixture writes and include the whole action, including later AQE planning.
          assert(activeMeasurements.compareAndSet(null, measurements))
          val rows = try {
            spark.experimental.extraStrategies = planner +: originalStrategies
            df.collect().toSeq
          } finally {
            activeMeasurements.set(null)
            spark.experimental.extraStrategies = originalStrategies
          }
          assert(rows == Seq(Row(depth.toLong)))
          val planningCalls = planner.counts.map(_._2).sum
          val columnarCalls = measurements.columnar.get()
          val adaptiveCalls = measurements.adaptive.get()
          val counts = s"adaptive=$adaptive broadcastOnly=$broadcastOnly " +
            s"depth=$depth planner=$planningCalls columnar=$columnarCalls aqe=$adaptiveCalls"
          logInfo(s"SPARK-48486 execution checkpoints: $counts")
          assert(columnarCalls > 0, "the columnar preparation checkpoint was not invoked")
          assert((adaptiveCalls > 0) == adaptive,
            "the AQE preparation checkpoint must run exactly in the adaptive cases")

          val executed = df.queryExecution.executedPlan
          if (adaptive) {
            assert(executed.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
          }
          assertPrunedScans(executed, depthFacts.take(depth), broadcastOnly)

          withClue(counts) {
            if (adaptive) {
              // AQE can replan during the action. The independent path can replan at each
              // stage, so allow a quadratic bound while still detecting repeated construction.
              val stageBound = (depth + 1) * (depth + 3)
              assert(adaptiveCalls <= (if (broadcastOnly) 4 * depth + 2 else stageBound))
              assert(columnarCalls <= (depth + 1) * (depth + 3))
              assert(planningCalls <= (if (broadcastOnly) 3 * depth + 2 else stageBound))
            } else {
              // The main query and each distinct DPP producer are prepared once. Independent
              // DPP additionally plans its aggregate, but does not prepare the comparison plan.
              assert(columnarCalls == depth + 1)
              assert(planningCalls == (if (broadcastOnly) depth + 1 else 2 * depth + 1))
            }
          }
        }
      }
    }
  }

  for {
    adaptive <- Seq(true, false)
    broadcastOnly <- Seq(true, false)
    reuse <- Seq(true, false)
  } {
    test(s"SPARK-48486: nested DPP, adaptive=$adaptive, " +
        s"broadcastOnly=$broadcastOnly, reuse=$reuse") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> broadcastOnly.toString,
          SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false",
          SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.0",
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
          SQLConf.SUBQUERY_REUSE_ENABLED.key -> reuse.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val query = if (broadcastOnly) {
          """
            |SELECT /*+ BROADCAST(q) */ sum(q.payload + c.payload)
            |FROM (
            |  SELECT /*+ BROADCAST(q) */ q.k, b.payload
            |  FROM (SELECT k FROM dpp_dim WHERE flag = 1) q
            |  JOIN dpp_b b ON q.k = b.k
            |) q JOIN dpp_c c ON q.k = c.k
            |""".stripMargin
        } else {
          """
            |SELECT sum(b.payload + c.payload)
            |FROM dpp_dim a
            |LEFT JOIN dpp_b b ON a.k = b.k
            |LEFT JOIN dpp_c c ON a.k = c.k
            |WHERE a.flag = 1
            |""".stripMargin
        }
        val df = sql(query)
        val dpp = df.queryExecution.optimizedPlan.flatMap { node =>
          node.expressions.flatMap(_.collect { case d: DynamicPruningSubquery => d })
        }
        assert(dpp.size == 2)
        assert(dpp.forall(_.onlyInBroadcast == broadcastOnly))

        val counter = new PlanningCounter
        val originalStrategies = spark.experimental.extraStrategies
        try {
          spark.experimental.extraStrategies = counter +: originalStrategies
          df.queryExecution.executedPlan
        } finally {
          spark.experimental.extraStrategies = originalStrategies
        }

        val counts = counter.counts
        logInfo(s"SPARK-48486 initial planning counts: ${counts.map(_._2)}")
        assert(counts.size == (if (adaptive || broadcastOnly) 3 else 5))
        if (reuse) {
          assert(counts.forall(_._2 == 1),
            counts.filter(_._2 > 1).map { case (plan, count) =>
              s"Planned $count times:\n$plan"
            }.mkString("\n"))
        } else {
          assert(counts.exists(_._2 > 1), "disabled reuse should retain independent construction")
        }
        checkAnswer(df, Row(15120000L))
        assertPrunedScans(df.queryExecution.executedPlan, Seq("dpp_b", "dpp_c"), broadcastOnly)
      }
    }
  }

  test("SPARK-48486: nested AQE compilation grows with distinct build queries") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      val joins = (1 to 4).map(i => s"LEFT JOIN dpp_b b$i ON a.k = b$i.k")
      val sum = (1 to 4).map(i => s"b$i.payload").mkString(" + ")
      for (reuse <- Seq(false, true)) {
        withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> reuse.toString) {
          val df = sql(s"SELECT sum($sum) FROM dpp_dim a " +
            joins.mkString(" ") + " WHERE a.flag = 1")
          val counter = new PlanningCounter
          val originalStrategies = spark.experimental.extraStrategies
          try {
            spark.experimental.extraStrategies = counter +: originalStrategies
            df.queryExecution.executedPlan
          } finally {
            spark.experimental.extraStrategies = originalStrategies
          }
          val counts = counter.counts
          logInfo(s"SPARK-48486 four-join planning, reuse=$reuse: ${counts.map(_._2)}")
          assert(counts.size == 5)
          assert(counts.map(_._2).sum == (if (reuse) 5 else 16))
        }
      }
    }
  }

  private class PlanningCounter extends SparkStrategy {
    private val planned = mutable.HashMap.empty[LogicalPlan, Int]

    def counts: Seq[(LogicalPlan, Int)] = synchronized { planned.toSeq }

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case ReturnAnswer(query) => synchronized {
          val key = query.canonicalized
          planned.update(key, planned.getOrElse(key, 0) + 1)
        }
        case _ =>
      }
      Nil
    }
  }

  private def unwrapSubquery(plan: BaseSubqueryExec): BaseSubqueryExec = plan match {
    case reused: ReusedSubqueryExec => unwrapSubquery(reused.child)
    case other => other
  }

  private def assertPrunedScans(
      plan: SparkPlan,
      tables: Seq[String],
      broadcastOnly: Boolean): Unit = {
    val scans = mainQueryScans(plan)
    for (table <- tables) {
      val matching = scans.filter(_.tableIdentifier.exists(_.table == table))
      assert(matching.nonEmpty, s"missing executed scan for $table")
      matching.foreach { scan =>
        val filters = scan.partitionFilters.collect {
          case d: DynamicPruningExpression => d.child
        }
        assert(filters.size == 1, s"expected DPP for $table: ${scan.partitionFilters}")
        val subquery = filters.head match {
          case in: InSubqueryExec => in
          case other => fail(s"DPP for $table was disabled or unresolved: $other")
        }
        assert(subquery.values().exists(_.toSeq == Seq(0L)))
        val producer = unwrapSubquery(subquery.plan)
        if (broadcastOnly) {
          assert(producer.isInstanceOf[SubqueryBroadcastExec])
        } else {
          assert(producer.isInstanceOf[SubqueryExec])
        }
        assert(scan.metrics("numPartitions").value == 1L)
      }
    }
  }

  private def mainQueryScans(plan: SparkPlan): Seq[FileSourceScanExec] = {
    collect(plan) {
      case scan: FileSourceScanExec => Seq(scan)
      case reused: ReusedExchangeExec => mainQueryScans(reused.child)
    }.flatten
  }

  // These count calls at preparation checkpoints, not all TreeNode visits. Reuse's recursive
  // traversal is checked separately by ReuseExchangeAndSubquerySuite. Neither test bounds plan
  // rendering, SparkPlanInfo construction or SQL event serialization.
  private class PreparationMeasurements {
    val columnar = new AtomicLong
    val adaptive = new AtomicLong
  }
}
