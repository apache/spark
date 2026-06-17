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
package org.apache.spark.sql.execution.metric

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.Tag

import org.apache.spark.internal.config
import org.apache.spark.sql.execution.{CollectLimitExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQETestHelper, DisableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SQLLastAttemptMetricPlanShapesSuite
  extends SharedSparkSession
  with SQLMetricsTestUtils
  // Need to control AQE per-test to ensure expected plan shapes.
  with DisableAdaptiveExecutionSuite {

  import testImplicits._

  import SQLLastAttemptMetricPlanShapesSuite._

  // Avoid initialising this before the Spark Context is initialised.
  protected var testSLAMetric: SQLLastAttemptMetric = _

  protected def setUpTestTable(): Unit = {
    val rand = new Random(1)
    val randomPrefix = rand.nextString(30)
    spark
      .range(NUM_RECORDS)
      .map { id =>
        (id, (id % LOW_CARDINALITY).toInt, randomPrefix + (id % LARGE_CARDINALITY))
      }.toDF("id", "low_cardinality_col", "large_col")
      .write.format("parquet").saveAsTable(TABLE_NAME)
    val numRecords = spark.read.table(TABLE_NAME).count()
    assert(numRecords === 300)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setUpTestTable()
    testSLAMetric = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
    // Move this into a local field so the closure doesn't hang on to the whole `this`
    // reference as well.
    val metric = testSLAMetric
    val incrementMetric = () => { metric += 1; true }
    val incrementMetricUdf = udf(incrementMetric).asNondeterministic()
    spark.udf.register("increment_metric", incrementMetricUdf)
  }

  override protected def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $TABLE_NAME")
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // note: reset() does not influence lastAttemptValue, but influences regular value
    testSLAMetric.reset()
  }

  object MetricValue {
    type Check = Option[Long] => Unit

    // Having the asserts in these helpers instead of in testPhysicalPlanShape
    // produces better error messages.
    def exactly(expectedValue: Long): Check = actualValue =>
      assert(actualValue === Some(expectedValue))

    def atLeast(minimumValue: Long): Check = { actualValue =>
      assert(actualValue.isDefined)
      assert(actualValue.get >= minimumValue)
    }
  }

  object PhysicalPlan {
    type Check = SparkPlan => Unit

    val ANY: Check = _ => () //  Ignore.

    def contains[T <: SparkPlan: ClassTag](implicit cls: ClassTag[T]): Check = { plan =>
      val existsSomeNodeOfTypeT =
        AdaptiveSparkPlanHelper.existsWithSubqueries(plan)(_.getClass == cls.runtimeClass)
      assert(
        existsSomeNodeOfTypeT,
        s"Expected a node ${cls.runtimeClass.getSimpleName}. Actual Plan:\n${plan.treeString}")
    }

    def exists(pf: PartialFunction[SparkPlan, Boolean]): Check = { plan =>
      val existsMatchingNode =
        AdaptiveSparkPlanHelper.existsWithSubqueries(plan)(pf.lift(_).getOrElse(false))
      assert(
        existsMatchingNode,
        s"Unexpected plan (check match function). Actual Plan:\n${plan.treeString}")
    }

    def isAQE: Boolean = SQLConf.get.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)

    def hasStageRetries: Boolean = spark.sparkContext.conf
      .getOption(config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key).contains("true")

    def hasAQEReplans: Boolean = AQETestHelper.isForcedCancellationEnabled
  }

  protected def testPhysicalPlanShape(
      label: String,
      setup: () => Unit = () => (),
      extraSQLConfs: Map[String, String] = Map.empty,
      sqlQuery: String,
      executedPlanCheck: PhysicalPlan.Check,
      metricValueCheck: MetricValue.Check
  )(testTags: Tag*): Unit = {
    for {
      useAQE <- BOOLEAN_DOMAIN
      stageRetries <- BOOLEAN_DOMAIN
      aqeReplans <- if (useAQE) BOOLEAN_DOMAIN else Seq(false)
    } test(s"$label - " +
        s"useAQE=$useAQE, stageRetries=$stageRetries, aqeReplans=$aqeReplans",
        testTags: _*) {

      // There is some special handling for df.cache() / df.persist() / df.localCheckpoint() tests.
      val cachedPlanTest = label.startsWith("cache - ")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> useAQE.toString) {
        setup()
        withSQLConf(extraSQLConfs.toSeq: _*) {
          val aqeRetryMetrics = if (aqeReplans) Seq(testSLAMetric) else Seq.empty
          AQETestHelper.withForcedCancellation(aqeRetryMetrics: _*) {
            withSparkContextConf(
                config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> stageRetries.toString) {
              val resultDf = spark.sql(sqlQuery)
              val _ = resultDf.collect()

              // normal value of the metrics shall not work with retries or replans
              if (!stageRetries && !aqeReplans) {
                metricValueCheck(Some(testSLAMetric.value))
              }
              // test LastRDDValue
              metricValueCheck(testSLAMetric.lastAttemptValueForHighestRDDId())
              // test Dataset value
              if (!cachedPlanTest) {
                // SLAM.lastAttemptValueForDataset is undefined when SLAM is inside
                // cached or checkpointed plan.
                metricValueCheck(testSLAMetric.lastAttemptValueForDataset(resultDf))
              }
              // test expected plan shape
              val executedPlan = resultDf.queryExecution.executedPlan
              executedPlanCheck(executedPlan)
              val rddIdExec = testSLAMetric.getHighestRDDId

              // Repeated execution should not affect SLAM metric value
              resultDf.collect()
              // test LastRDDValue again
              metricValueCheck(testSLAMetric.lastAttemptValueForHighestRDDId())
              // test Dataset value again
              if (!cachedPlanTest) {
                // SLAM.lastAttemptValueForDataset is undefined when SLAM is inside
                // cached or checkpointed plan.
                metricValueCheck(testSLAMetric.lastAttemptValueForDataset(resultDf))
              }

              // count() transformation creates a new Dataset.
              // It should not affect the SLAM metric value of the first Dataset.
              resultDf.count()
              // test Dataset value again
              if (!cachedPlanTest) {
                // SLAM.lastAttemptValueForDataset is undefined when SLAM is inside
                // cached or checkpointed plan.
                metricValueCheck(testSLAMetric.lastAttemptValueForDataset(resultDf))
              }
              // This should have created a new plan and executed new RDDs,
              // unless it's a test of cached plan.
              val rddIdExecCount = testSLAMetric.getHighestRDDId
              if (cachedPlanTest) {
                assert(rddIdExecCount === rddIdExec)
              } else {
                // count() creates a new plan with new RDDs.
                assert(rddIdExecCount.get > rddIdExec.get)
              }
            }
          }
        }
      }
    }
  }

  protected def testPlanShape(
      label: String,
      sqlQuery: String,
      // Assert on the result of the test metric.
      metricValueCheck: MetricValue.Check,
      testTags: Tag*
  ): Unit = {
    testPhysicalPlanShape(
      label = label,
      sqlQuery = sqlQuery,
      executedPlanCheck = PhysicalPlan.ANY,
      metricValueCheck = metricValueCheck
    )(testTags: _*)
  }

  testPlanShape(
    label = "simple plan",
    sqlQuery = s"SELECT * FROM $TABLE_NAME WHERE increment_metric()",
    metricValueCheck = MetricValue.exactly(NUM_RECORDS)
  )

  /* ********************
  * Various Subquery Plans
  * ********************** */
  testPlanShape(
    label = "subquery - IN",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE id IN (
         |   SELECT low_cardinality_col
         |   FROM $TABLE_NAME
         |   WHERE increment_metric())""".stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS)
  )

  testPlanShape(
    label = "subquery - IN - aggregation",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE id IN (
         |   SELECT DISTINCT(low_cardinality_col)
         |   FROM $TABLE_NAME
         |   WHERE increment_metric())""".stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS)
  )

  testPlanShape(
    label = "subquery - IN - TVF",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE id IN (
         |   SELECT *
         |   FROM range(5)
         |   WHERE increment_metric())""".stripMargin,
    metricValueCheck = MetricValue.exactly(5)
  )

  testPlanShape(
    label = "subquery - IN - explode",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE id IN (
         |   SELECT explode(array(low_cardinality_col, low_cardinality_col + 1))
         |   FROM $TABLE_NAME
         |   WHERE increment_metric())""".stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS)
  )

  testPlanShape(
    label = "subquery - IN - lateral view explode",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE id IN (
         |   SELECT new_column
         |   FROM $TABLE_NAME LATERAL VIEW
         |     explode(array(low_cardinality_col, low_cardinality_col + 1)) AS new_column
         |   WHERE increment_metric())""".stripMargin,
    metricValueCheck = MetricValue.exactly(2 * NUM_RECORDS)
  )

  testPlanShape(
    label = "subquery - scalar",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE id == (
         |   SELECT MAX(low_cardinality_col)
         |   FROM $TABLE_NAME
         |   WHERE increment_metric())""".stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS)
  )

  testPhysicalPlanShape(
    label = "subquery - EXISTS",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME
         | WHERE EXISTS (
         |   SELECT low_cardinality_col
         |   FROM $TABLE_NAME
         |   WHERE increment_metric())""".stripMargin,
    // This turns into a LIMIT query.
    metricValueCheck = MetricValue.atLeast(1),
    executedPlanCheck = PhysicalPlan.contains[CollectLimitExec]
  )()

  testPhysicalPlanShape(
    label = "subquery - EXISTS (correlated)",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME outer_table
         | WHERE EXISTS (
         |   SELECT low_cardinality_col
         |   FROM $TABLE_NAME inner_table
         |   WHERE increment_metric()
         |     AND inner_table.low_cardinality_col == outer_table.low_cardinality_col)
         |     """.stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS),
    executedPlanCheck = PhysicalPlan.exists {
      case _: BroadcastExchangeExec => true
      case _: ShuffleExchangeExec => true
      case _: ReusedExchangeExec => true
    }
  )()

  /* *****************************
  * Plans with different Exchanges
  * ****************************** */

  /*
   * To cover:
   * - ShuffleExchangeLike
   *   - ShuffleExchangeExec: covered by exchange - Shuffle
   * - ReusedExchangeExec: covered by exchange - ReusedExchangeExec
   * - BroadcastExchangeLike:
   *   - BroadcastExchangeExec: covered above by subquery - EXISTS (correlated))
   * - InMemoryTableScanLike (InMemoryTableScanExec): covered by exchange - InMemoryTableScanExec
   */

  testPhysicalPlanShape(
    label = "exchange - Shuffle",
    sqlQuery =
      s"""SELECT *
         | FROM $TABLE_NAME orig
         | FULL OUTER JOIN (
         |   SELECT *
         |   FROM $TABLE_NAME
         |   WHERE increment_metric()
         | ) with_metric USING (id)""".stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS),
    executedPlanCheck = PhysicalPlan.exists {
      case _: ShuffleExchangeExec => true
      // After forced AQE replans it may use ReusedExchange.
      case _: ReusedExchangeExec if PhysicalPlan.hasAQEReplans => true
    }
  )()

  testPhysicalPlanShape(
    label = "exchange - ReusedExchangeExec",
    sqlQuery =
      s"""WITH subquery_with_metric AS (
        |   SELECT *
        |   FROM $TABLE_NAME
        |   WHERE increment_metric()
        | )
        |SELECT *
        | FROM subquery_with_metric a JOIN subquery_with_metric b USING (id)""".stripMargin,
    metricValueCheck = MetricValue.exactly(NUM_RECORDS),
    executedPlanCheck = PhysicalPlan.contains[ReusedExchangeExec]
  )()

  for (eager <- Seq("true", "false", "manual")) {
    // SLAM metric in the top stage of cached query.
    testPhysicalPlanShape(
      label = s"cache - InMemoryTableScanExec - result stage - eager=$eager",
      setup = () => {
        spark.sql(s"""
           |CREATE OR REPLACE TEMP VIEW table_with_metric AS (
           |  SELECT low_cardinality_col
           |  FROM $TABLE_NAME
           |  WHERE increment_metric()
           |)""".stripMargin)
        if (eager == "true") {
          spark.sql("CACHE TABLE table_with_metric")
        } else { // false or manual
          spark.sql("CACHE LAZY TABLE table_with_metric")
        }
        if (eager == "manual") {
          spark.sql("select count(*) from table_with_metric").collect()
        }
      },
      sqlQuery =
        s"""SELECT *
           | FROM $TABLE_NAME
           | WHERE id IN (SELECT * FROM table_with_metric)""".stripMargin,
      metricValueCheck = MetricValue.exactly(NUM_RECORDS),
      executedPlanCheck = PhysicalPlan.contains[InMemoryTableScanExec]
    )()

    // SLAM metric in the map stage of cached query.
    testPhysicalPlanShape(
      label = s"cache - InMemoryTableScanExec - map stage - eager=$eager",
      setup = () => {
        spark.sql(s"""
          |CREATE OR REPLACE TEMP VIEW table_with_metric AS (
          |  SELECT id, SUM(low_cardinality_col)
          |  FROM $TABLE_NAME
          |  WHERE increment_metric()
          |  GROUP BY id
          |)""".stripMargin)
        if (eager == "true") {
          spark.sql("CACHE TABLE table_with_metric")
        } else { // false or manual
          spark.sql("CACHE LAZY TABLE table_with_metric")
        }
        if (eager == "manual") {
          spark.sql("select count(*) from table_with_metric").collect()
        }
      },
      sqlQuery =
        s"""SELECT *
           | FROM table_with_metric""".stripMargin,
      metricValueCheck = MetricValue.exactly(NUM_RECORDS),
      executedPlanCheck = PhysicalPlan.contains[InMemoryTableScanExec]
    )()

    testPhysicalPlanShape(
      label = s"cache - localCheckpoint - result stage - eager=$eager",
      setup = () => {
        val df = spark.sql(s"""
            |SELECT low_cardinality_col
            |FROM $TABLE_NAME
            |WHERE increment_metric()""".stripMargin)
        val cpEager = if (eager == "true") true else false
        val cpDf = df.localCheckpoint(eager = cpEager)
        if (eager == "manual") {
          cpDf.count()
        }
        cpDf.createOrReplaceTempView("cp_table_with_metric")
      },
      sqlQuery =
        s"""SELECT *
           | FROM cp_table_with_metric""".stripMargin,
      metricValueCheck = MetricValue.exactly(NUM_RECORDS),
      executedPlanCheck = PhysicalPlan.contains[RDDScanExec]
    )()

    testPhysicalPlanShape(
      label = s"cache - localCheckpoint - map stage - eager=$eager",
      setup = () => {
        val df = spark.sql(s"""
          |SELECT id, SUM(low_cardinality_col)
          |FROM $TABLE_NAME
          |WHERE increment_metric()
          |GROUP BY id""".stripMargin)
        val cpEager = if (eager == "true") true else false
        val cpDf = df.localCheckpoint(eager = cpEager)
        if (eager == "manual") {
          cpDf.count()
        }
        cpDf.createOrReplaceTempView("cp_table_with_metric")
      },
      sqlQuery =
        s"""SELECT *
           | FROM cp_table_with_metric""".stripMargin,
      metricValueCheck = MetricValue.exactly(NUM_RECORDS),
      executedPlanCheck = PhysicalPlan.contains[RDDScanExec]
    )()
  }
}

object SQLLastAttemptMetricPlanShapesSuite {
  val NUM_RECORDS: Long = 300
  val LOW_CARDINALITY: Int = 5
  val LARGE_CARDINALITY: Int = 111

  val TABLE_NAME: String = "test_table"
}
