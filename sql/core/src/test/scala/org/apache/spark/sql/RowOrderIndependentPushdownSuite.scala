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

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, DynamicPruningExpression, DynamicPruningSubquery}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, LocalLimit, Project}
import org.apache.spark.sql.execution.{CollectLimitExec, FileSourceScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class RowOrderIndependentPushdownSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def withRoiMaskFunction(f: => Unit): Unit = {
    spark.sessionState.functionRegistry.createOrReplaceTempFunction(
      "roi_mask",
      children => StaticInvoke(
        classOf[java.lang.Math],
        IntegerType,
        "abs",
        children,
        isDeterministic = false,
        isRowOrderIndependent = true),
      "scala_udf")
    withUserDefinedFunction("roi_mask" -> true) {
      f
    }
  }

  private def hasDppInScan(df: DataFrame): Boolean = {
    collect(df.queryExecution.executedPlan) {
      case scan: FileSourceScanExec =>
        scan.partitionFilters.exists(_.exists(_.isInstanceOf[DynamicPruningExpression]))
    }.exists(identity)
  }

  test("DPP pushes through row-order-independent Project with stay-up predicate and " +
    "survives CleanupDynamicPruningFilters") {
    withRoiMaskFunction {
      withTable("roi_fact", "roi_dim") {
        (1 to 5).flatMap(dt => (1 to 4).map(i => (dt * 10 + i, dt * 100 + i, dt)))
          .toDF("id", "payload", "dt")
          .write
          .partitionBy("dt")
          .format("parquet")
          .saveAsTable("roi_fact")

        Seq((1, "EU"), (2, "US"), (3, "APAC"), (4, "LATAM"), (5, "MEA"))
          .toDF("dt", "region")
          .write
          .format("parquet")
          .saveAsTable("roi_dim")

        withSQLConf(
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
            SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val query =
            """
              |SELECT f.id, f.dt, f.enc
              |FROM (
              |  SELECT id, dt, roi_mask(payload) AS enc
              |  FROM roi_fact
              |) f
              |JOIN roi_dim d ON f.dt = d.dt
              |WHERE f.enc IS NOT NULL AND d.region = 'US'
            """.stripMargin

          val df = sql(query)
          val optimized = df.queryExecution.optimizedPlan

          // DPP must be pushed below the ROI Project and survive CleanupDynamicPruningFilters.
          val pushedDppBelowRoiProject = optimized.exists {
            case Project(pList, child) if pList.exists(_.exists {
                case s: StaticInvoke => !s.isDeterministic && s.isRowOrderIndependent
                case _ => false
              }) =>
              child.exists {
                case Filter(cond, _) => cond.exists(_.isInstanceOf[DynamicPruningSubquery])
                case _ => false
              }
            case _ => false
          }
          assert(pushedDppBelowRoiProject,
            s"Expected DynamicPruningSubquery below ROI Project in:\n$optimized")
          assert(hasDppInScan(df),
            s"Expected DynamicPruningExpression in FileSourceScanExec:\n${df.queryExecution}")

          checkAnswer(
            df,
            Seq(Row(21, 2, 201), Row(22, 2, 202), Row(23, 2, 203), Row(24, 2, 204)))

          val factScan = collectFirst(df.queryExecution.executedPlan) {
            case s: FileSourceScanExec if s.partitionFilters.nonEmpty => s
          }.get
          assert(factScan.metrics("numPartitions").value === 1)

          // Kill-switch: disabling config prevents DPP from pushing past ROI Project.
          withSQLConf(
              SQLConf.ROW_ORDER_INDEPENDENT_NONDETERMINISTIC_PUSHDOWN_ENABLED.key -> "false") {
            val disabledDf = sql(query)
            assert(!hasDppInScan(disabledDf),
              s"Expected no DPP when kill-switch is false:\n${disabledDf.queryExecution}")
          }

          // Negative: stateful Rand(1) projection must NOT allow DPP pushdown.
          val randDf = sql(
            """
              |SELECT f.id, f.dt, f.enc
              |FROM (
              |  SELECT id, dt, rand(1) AS enc
              |  FROM roi_fact
              |) f
              |JOIN roi_dim d ON f.dt = d.dt
              |WHERE f.enc IS NOT NULL AND d.region = 'US'
            """.stripMargin)
          assert(!hasDppInScan(randDf),
            s"Expected no DPP below Project(rand(1)):\n${randDf.queryExecution}")
        }
      }
    }
  }

  test("DPP pushes through row-order-independent Aggregate with stay-up predicate") {
    withRoiMaskFunction {
      withTable("roi_agg_fact", "roi_agg_dim") {
        (1 to 5).flatMap(dt => (1 to 4).map(i => (dt * 10 + i, i, dt)))
          .toDF("id", "payload", "dt")
          .write
          .partitionBy("dt")
          .format("parquet")
          .saveAsTable("roi_agg_fact")

        Seq((1, "EU"), (2, "US"), (3, "APAC"), (4, "LATAM"), (5, "MEA"))
          .toDF("dt", "region")
          .write
          .format("parquet")
          .saveAsTable("roi_agg_dim")

        withSQLConf(
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
            SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val aggDf = sql(
            """
              |SELECT f.dt, f.enc_sum
              |FROM (
              |  SELECT dt, roi_mask(CAST(SUM(payload) AS INT)) AS enc_sum
              |  FROM roi_agg_fact
              |  GROUP BY dt
              |) f
              |JOIN roi_agg_dim d ON f.dt = d.dt
              |WHERE f.enc_sum IS NOT NULL AND d.region = 'US'
            """.stripMargin)

          val optimized = aggDf.queryExecution.optimizedPlan
          val pushedDppBelowRoiAgg = optimized.exists {
            case agg: Aggregate if agg.aggregateExpressions.exists(_.exists {
                case s: StaticInvoke => !s.isDeterministic && s.isRowOrderIndependent
                case _ => false
              }) =>
              agg.child.exists {
                case Filter(cond, _) => cond.exists(_.isInstanceOf[DynamicPruningSubquery])
                case _ => false
              }
            case _ => false
          }
          assert(pushedDppBelowRoiAgg,
            s"Expected DynamicPruningSubquery below ROI Aggregate in:\n$optimized")
          assert(hasDppInScan(aggDf),
            s"Expected DynamicPruningExpression in FileSourceScanExec:\n${aggDf.queryExecution}")
          checkAnswer(aggDf, Seq(Row(2, 10)))

          // Negative: stateful Rand(1) in Aggregate blocks DPP pushdown.
          val randAggDf = sql(
            """
              |SELECT f.dt, f.r
              |FROM (
              |  SELECT dt, rand(1) AS r
              |  FROM roi_agg_fact
              |  GROUP BY dt
              |) f
              |JOIN roi_agg_dim d ON f.dt = d.dt
              |WHERE f.r IS NOT NULL AND d.region = 'US'
            """.stripMargin)
          assert(!hasDppInScan(randAggDf),
            s"Expected no DPP below Aggregate(rand(1)):\n${randAggDf.queryExecution}")
        }
      }
    }
  }

  test("InjectRuntimeFilter BloomFilter pushes below row-order-independent Project past " +
    "stay-up predicate") {
    withRoiMaskFunction {
      withTable("roi_bf_fact", "roi_bf_dim") {
        (1 to 50).map(i => (i, i * 10))
          .toDF("id", "payload")
          .write
          .format("parquet")
          .saveAsTable("roi_bf_fact")

        Seq((7, "target"), (8, "other"), (9, "other"))
          .toDF("id", "tag")
          .write
          .format("parquet")
          .saveAsTable("roi_bf_dim")

        withSQLConf(
            SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
            SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val bfDf = sql(
            """
              |SELECT f.id, f.enc
              |FROM (
              |  SELECT id, roi_mask(payload) AS enc
              |  FROM roi_bf_fact
              |) f
              |JOIN roi_bf_dim d ON f.id = d.id
              |WHERE f.enc IS NOT NULL AND d.tag = 'target'
            """.stripMargin)

          val optimized = bfDf.queryExecution.optimizedPlan
          val pushedBloomBelowRoiProject = optimized.exists {
            case Project(pList, child) if pList.exists(_.exists {
                case s: StaticInvoke => !s.isDeterministic && s.isRowOrderIndependent
                case _ => false
              }) =>
              child.exists {
                case Filter(cond, _) => cond.exists(_.isInstanceOf[BloomFilterMightContain])
                case _ => false
              }
            case _ => false
          }
          assert(pushedBloomBelowRoiProject,
            s"Expected BloomFilterMightContain below ROI Project in:\n$optimized")
          checkAnswer(bfDf, Seq(Row(7, 70)))

          // Negative: stateful Rand(1) Project must NOT let BloomFilter push below it.
          val randBfDf = sql(
            """
              |SELECT f.id, f.r
              |FROM (
              |  SELECT id, rand(1) AS r
              |  FROM roi_bf_fact
              |) f
              |JOIN roi_bf_dim d ON f.id = d.id
              |WHERE f.r IS NOT NULL AND d.tag = 'target'
            """.stripMargin)
          val randOptimized = randBfDf.queryExecution.optimizedPlan
          val pushedBloomBelowRandProject = randOptimized.exists {
            case Project(pList, child) if pList.exists(!_.deterministic) =>
              child.exists {
                case Filter(cond, _) => cond.exists(_.isInstanceOf[BloomFilterMightContain])
                case _ => false
              }
            case _ => false
          }
          assert(!pushedBloomBelowRandProject,
            s"Expected BloomFilterMightContain NOT pushed below Project(rand(1)):\n$randOptimized")
        }
      }
    }
  }

  private def withCountingRoiMaskFunction(f: => Unit): Unit = {
    spark.sessionState.functionRegistry.createOrReplaceTempFunction(
      "roi_mask",
      children => StaticInvoke(
        RowOrderIndependentPushdownSuite.getClass,
        IntegerType,
        "countAndMask",
        children,
        isDeterministic = false,
        isRowOrderIndependent = true),
      "scala_udf")
    withUserDefinedFunction("roi_mask" -> true) {
      f
    }
  }

  test("row-order-independent StaticInvoke stays above GlobalLimit on multi-partition table") {
    withCountingRoiMaskFunction {
      withTable("roi_limit_tbl") {
        withSQLConf(
            SQLConf.ROW_ORDER_INDEPENDENT_NONDETERMINISTIC_PUSHDOWN_ENABLED.key -> "true",
            SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1",
            SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1") {
          (1 to 40).map(i => (i, -i))
            .toDF("id", "v")
            .repartition(4)
            .write
            .format("parquet")
            .saveAsTable("roi_limit_tbl")

          val scanPartitions = sql("SELECT id, v FROM roi_limit_tbl").rdd.getNumPartitions
          assert(scanPartitions >= 4, s"Expected >= 4 scan partitions, got $scanPartitions")

          val limitQuery =
            "SELECT id, roi_mask(v) AS enc FROM (SELECT id, v FROM roi_limit_tbl LIMIT 5)"
          val roiDf = sql(limitQuery)
          val optimized = roiDf.queryExecution.optimizedPlan

          // Project(roi_mask) must stay above GlobalLimit(5) and never push below Limit.
          val roiAboveGlobalLimit = optimized.exists {
            case Project(pList, child) if pList.exists(_.exists {
                case s: StaticInvoke => !s.isDeterministic && s.isRowOrderIndependent
                case _ => false
              }) =>
              child.exists(_.isInstanceOf[GlobalLimit])
            case _ => false
          }
          val roiBelowLimit = optimized.exists {
            case l @ (_: GlobalLimit | _: LocalLimit) =>
              l.children.exists(_.exists {
                case p: Project =>
                  p.projectList.exists(_.exists {
                    case s: StaticInvoke => !s.isDeterministic && s.isRowOrderIndependent
                    case _ => false
                  })
                case _ => false
              })
            case _ => false
          }
          assert(roiAboveGlobalLimit,
            s"Expected ROI Project above GlobalLimit in:\n$optimized")
          assert(!roiBelowLimit,
            s"Expected ROI Project NOT pushed below Limit in:\n$optimized")
          assert(collect(roiDf.queryExecution.executedPlan) {
            case c: CollectLimitExec => c
          }.isEmpty, s"Expected no CollectLimitExec above ROI Project in:\n" +
            s"${roiDf.queryExecution.executedPlan}")

          // Direct execution evaluates roi_mask only on the 5 globally surviving rows.
          RowOrderIndependentPushdownSuite.maskInvocations.set(0)
          val rows = roiDf.collect()
          assert(rows.length === 5)
          assert(rows.forall(r => r.getInt(1) === r.getInt(0)))
          assert(RowOrderIndependentPushdownSuite.maskInvocations.get() === 5)

          // Non-root limit (where LocalLimitExec runs across all partitions before GlobalLimit)
          // must still evaluate roi_mask only 5 times, not up to 5 * numPartitions.
          RowOrderIndependentPushdownSuite.maskInvocations.set(0)
          val nonRootDf = sql(s"SELECT COUNT(*), SUM(enc) FROM ($limitQuery)")
          val aggRow = nonRootDf.collect().head
          assert(aggRow.getLong(0) === 5L)
          assert(RowOrderIndependentPushdownSuite.maskInvocations.get() === 5)
        }
      }
    }
  }

  test("static Parquet view partition pruning, PushedFilters, and ANSI barrier") {
    withCountingRoiMaskFunction {
      withTable("roi_events") {
        withView("roi_scored") {
          (1 to 5).flatMap { dt =>
            Seq("US", "EU").flatMap { country =>
              (0 to 3).map(i => (dt * 10 + i, country, -i, dt))
            }
          }.toDF("id", "country", "v", "dt")
            .write
            .partitionBy("dt")
            .format("parquet")
            .saveAsTable("roi_events")

          sql("CREATE TEMP VIEW roi_scored AS " +
            "SELECT id, country, dt, v, roi_mask(v) AS enc FROM roi_events")

          withSQLConf(
              SQLConf.ROW_ORDER_INDEPENDENT_NONDETERMINISTIC_PUSHDOWN_ENABLED.key -> "true") {
            RowOrderIndependentPushdownSuite.maskInvocations.set(0)
            val df = sql(
              "SELECT id, enc FROM roi_scored WHERE dt = 3 AND country = 'US'")
            val rows = df.collect()
            assert(rows.length === 4)
            assert(RowOrderIndependentPushdownSuite.maskInvocations.get() === 4)

            val scans = collect(df.queryExecution.executedPlan) {
              case s: FileSourceScanExec => s
            }
            assert(scans.size === 1)
            assert(scans.head.partitionFilters.nonEmpty,
              s"Expected static PartitionFilters in ${scans.head}")
            assert(scans.head.dataFilters.nonEmpty,
              s"Expected pushed DataFilters in ${scans.head}")
          }

          // ANSI barrier: post-projection 10 / v > 1 stays guarded by enc > 0 (v = 0 rows are
          // filtered out by enc > 0 before 10 / v executes, avoiding DIVIDE_BY_ZERO).
          withSQLConf(
              SQLConf.ROW_ORDER_INDEPENDENT_NONDETERMINISTIC_PUSHDOWN_ENABLED.key -> "true",
              SQLConf.ANSI_ENABLED.key -> "true") {
            val ansiRows = sql(
              "SELECT id, enc FROM roi_scored " +
                "WHERE dt = 3 AND country = 'US' AND enc > 0 AND 10 / v > -100").collect()
            assert(ansiRows.length === 3)
          }
        }
      }
    }
  }
}

object RowOrderIndependentPushdownSuite {
  val maskInvocations = new AtomicInteger(0)

  def countAndMask(v: Int): Int = {
    maskInvocations.incrementAndGet()
    java.lang.Math.abs(v)
  }
}
