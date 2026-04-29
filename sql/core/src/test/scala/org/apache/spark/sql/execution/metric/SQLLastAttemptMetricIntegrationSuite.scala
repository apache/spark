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

import org.apache.spark.internal.config
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, CoalescedShuffleRead, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** Tests [[SQLLastAttemptMetric]] used by [[RDD]]s and [[Dataset]]s */
class SQLLastAttemptMetricIntegrationSuite
  extends SharedSparkSession
  with SQLMetricsTestUtils {
  import testImplicits._

  protected def withRetries = false

  test("single stage rdd updates with shared slam") {
    val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
    val rdd1 = spark.sparkContext.parallelize(1 to 10, 2).map { x =>
      slam.add(1)
      x
    }

    rdd1.count()
    assert(withRetries || slam.value === 10)
    assert(slam.lastAttemptValueForAllRDDs() === Some(10))
    assert(slam.lastAttemptValueForRDDId(rdd1.id) === Some(10))
    assert(slam.lastAttemptValueForRDDIds(Seq(rdd1.id, rdd1.id)) === Some(10))
    assert(slam.lastAttemptValueForRDDIds(Seq(rdd1.id + 1, rdd1.id + 2)) === Some(0))
    assert(slam.lastAttemptValueForRDDIds(Seq(rdd1.id, rdd1.id + 10, rdd1.id)) === Some(10))
    assert(slam.lastAttemptValueForHighestRDDId() === Some(10))

    val rdd2 = spark.sparkContext.parallelize(1 to 50, 3).map { x =>
      slam.add(3)
      x
    }
    rdd2.count()
    assert(withRetries || slam.value === 160) // +150
    assert(slam.lastAttemptValueForRDDId(rdd1.id) === Some(10)) // value for first rdd unaffected
    assert(slam.lastAttemptValueForRDDId(rdd2.id) === Some(150)) // value for second rdd recorded
    assert(slam.lastAttemptValueForAllRDDs() === Some(160)) // value for all rdds summed
    assert(slam.getNumRDDs === 2)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(150)) // highest RDD id updated.

    // Re-executing rdd1
    rdd1.count()
    assert(withRetries || slam.value === 170) // +10
    // Re-execution doesn't produce duplicate last attempt values
    assert(slam.lastAttemptValueForRDDId(rdd1.id) === Some(10))
    assert(slam.lastAttemptValueForAllRDDs() === Some(160))
    assert(slam.getNumRDDs === 2)
    // Highest RDD id tracks highest rdd.id, not the last RDD to be executed
    assert(slam.lastAttemptValueForHighestRDDId() === Some(150))

    // New RDD on top of rdd1, but in a single stage.
    val rdd3 = rdd1.map { x =>
      slam.add(2)
      x
    }
    rdd3.count()
    assert(withRetries || slam.value === 200) // +30
    assert(slam.lastAttemptValueForRDDId(rdd1.id) === Some(10)) // stays the same
    // The increment from rdd1 and rdd3 are in the same stage, so they are recorded together.
    assert(slam.getNumRDDs === 3)
    assert(slam.lastAttemptValueForRDDId(rdd3.id) === Some(30))
    assert(slam.getHighestRDDId === Some(rdd3.id))
    assert(slam.lastAttemptValueForHighestRDDId() === Some(30))

    // Setting a value directly from the driver makes slam bail out, because it can't reason
    // about what is the "last attempt" on driver vs. coming from RDD executions.
    slam.set(42)
    assert(!slam.getValid)
    // Information stays available for logging and debugging.
    assert(slam.getNumRDDs === 3)
    assert(slam.getHighestRDDId === Some(rdd3.id))

    logInfo(slam.logAccumulatorState)
  }

  test("multi stage rdd updates") {
    val slam1 = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM1")
    val slam2 = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM2")

    val rdd1 = spark.sparkContext.parallelize(1 to 10, 2).map { x =>
      slam1.add(1)
      x
    }
    val repartition = rdd1.repartition(10)
    val rdd2 = repartition.map { x =>
      slam2.add(1)
      x
    }
    rdd2.collect()
    assert(withRetries || slam1.value === 10)
    assert(withRetries || slam2.value === 10)
    assert(slam1.lastAttemptValueForAllRDDs() === Some(10))
    assert(slam2.lastAttemptValueForAllRDDs() === Some(10))
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10))
    assert(slam2.lastAttemptValueForHighestRDDId() === Some(10))
    // It is executed in a Stage submitted by the repartition.
    assert(slam1.lastAttemptValueForRDDId(rdd1.id) === Some(0))
    assert(slam1.lastAttemptValueForRDDId(repartition.id) === Some(0)) // Surprise, nope.
    // Repartition creates a number of MapPartitionsRDDs, CoalescedRDDs, ShuffledRDDs...
    // The actual stage that submits the map stage is somewhere internal.
    assert(slam1.getHighestRDDId.isDefined)
    val mapStageRddId = slam1.getHighestRDDId.get
    assert(slam1.lastAttemptValueForRDDId(mapStageRddId) === Some(10))

    // Test passing multiple ids.
    assert(slam1.lastAttemptValueForRDDIds(Seq(rdd1.id, repartition.id)) === Some(0))
    assert(slam1.lastAttemptValueForRDDIds(
      Seq(rdd1.id, mapStageRddId, repartition.id)) === Some(10))
    assert(slam1.lastAttemptValueForRDDIds(Seq(rdd1.id, rdd2.id)) === Some(0))
    assert(slam1.lastAttemptValueForRDDIds(Seq(-10)) === Some(0))

    rdd2.collect()
    // Repartition stage is reused, but result stage is re-executed.
    assert(withRetries || slam1.value === 10) // no change
    assert(withRetries || slam2.value === 20) // +10
    // Last attempt value is not duplicated, since result stage is an action on the same RDD.
    assert(slam1.lastAttemptValueForAllRDDs() === Some(10))
    assert(slam2.lastAttemptValueForAllRDDs() === Some(10))

    rdd1.collect()
    assert(withRetries || slam1.value === 20) // +10
    // The first time around it was executed in the repartition RDD stage.
    // This time around it is executed from action of rdd1.
    assert(slam1.getNumRDDs === 2)
    assert(slam1.lastAttemptValueForAllRDDs() === Some(20))
    assert(slam1.lastAttemptValueForRDDId(rdd1.id) === Some(10)) // new
    assert(slam1.lastAttemptValueForRDDId(mapStageRddId) === Some(10)) // old
    // Highest RDD id stays the same.
    assert(slam1.getHighestRDDId === Some(mapStageRddId))
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10))

    rdd1.collect()
    assert(withRetries || slam1.value === 30) // +10
    // Still the same
    assert(slam1.lastAttemptValueForAllRDDs() === Some(20))
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10))

    rdd2.collect()
    // Repartition stage is reused (again), but result stage is re-executed (again).
    assert(withRetries || slam1.value === 30) // no change
    assert(withRetries || slam2.value === 30) // +10
    // Last attempt value is not duplicated, since result stage is an action on the same RDD.
    assert(slam1.lastAttemptValueForAllRDDs() === Some(20))
    assert(slam2.lastAttemptValueForAllRDDs() === Some(10))

    // Executed in different RDDs, but never duplicated.
    assert(slam1.lastAttemptValueForRDDId(rdd1.id) === Some(10))
    assert(slam1.lastAttemptValueForRDDId(mapStageRddId) === Some(10))
    assert(slam1.lastAttemptValueForRDDId(repartition.id) === Some(0))
    assert(slam1.lastAttemptValueForRDDId(rdd2.id) === Some(0))
    assert(slam2.lastAttemptValueForRDDId(rdd1.id) === Some(0))
    assert(slam2.lastAttemptValueForRDDId(mapStageRddId) === Some(0))
    assert(slam1.lastAttemptValueForRDDId(repartition.id) === Some(0))
    assert(slam2.lastAttemptValueForRDDId(rdd2.id) === Some(10))

    val newRepartition = rdd1.repartition(10)
    val newRdd2 = newRepartition.map { x =>
      slam2.add(1)
      x
    }
    newRdd2.collect()
    assert(withRetries || slam1.value === 40) // +10
    assert(withRetries || slam2.value === 40) // +10
    // SLAM metrics get re-executed in the new RDDs
    // rdd1 is reused, but the shuffle is new, and that is what submits the map stage.
    assert(slam1.getNumRDDs === 3)
    assert(slam1.lastAttemptValueForAllRDDs() === Some(30)) // +10
    assert(slam2.getNumRDDs === 2)
    assert(slam2.lastAttemptValueForAllRDDs() === Some(20)) // +10
    // Values are recorded for the new highest RDD id.
    assert(slam1.getHighestRDDId.isDefined)
    val newMapStageId = slam1.getHighestRDDId.get
    assert(newMapStageId > mapStageRddId)
    assert(slam2.getHighestRDDId === Some(newRdd2.id))
    assert(slam1.lastAttemptValueForRDDId(newMapStageId) === Some(10))
    assert(slam2.lastAttemptValueForRDDId(newRdd2.id) === Some(10))
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10))
    assert(slam2.lastAttemptValueForHighestRDDId() === Some(10))

    logInfo(slam1.logAccumulatorState)
    logInfo(slam2.logAccumulatorState)
  }

  test("rdd take") {
    val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
    val rdd = spark.sparkContext.parallelize(1 to 100, 100).map { x =>
      slam.add(1)
      x
    }
    withSparkContextConf(
      // make it fixed to not be affected by potential changes of default.
      config.RDD_LIMIT_INITIAL_NUM_PARTITIONS.key -> "1",
      config.RDD_LIMIT_SCALE_UP_FACTOR.key -> "4"
    ) {
      rdd.take(1) // execute 1 partition
      assert(withRetries || slam.value === 1)
      assert(slam.lastAttemptValueForAllRDDs() === Some(1))

      // take(2) scales up from 1 partition; the exact number of partitions scanned
      // depends on the scale-up algorithm.
      val valueBefore = slam.value
      rdd.take(2)
      val slamAfterTake2 = slam.lastAttemptValueForAllRDDs()
      assert(slamAfterTake2.isDefined)
      assert(slamAfterTake2.get >= 2) // at least 2 partitions
      assert(slamAfterTake2.get < 100) // but not all partitions.

      // take(100) should execute all 100 partitions
      rdd.take(100)
      assert(slam.lastAttemptValueForAllRDDs() === Some(100))
      assert(slam.getNumRDDs === 1)
    }

    logInfo(slam.logAccumulatorState)
  }

  test("rdd coalesce") {
    val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
    val rdd1 = spark.sparkContext.parallelize(1 to 100, 100).map { x =>
      slam.add(1)
      x
    }
    val rdd2 = rdd1.coalesce(20)
    // Test that coalescing that changes partition count doesn't break anything.
    rdd2.collect()
    assert(slam.lastAttemptValueForRDDId(rdd2.id) === Some(100))
    rdd1.collect()
    assert(slam.lastAttemptValueForRDDId(rdd1.id) === Some(100))

    logInfo(slam.logAccumulatorState)
  }

  test("dataset updates") {
    val slam1 = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM1")
    val df1 = spark.range(10).filter(Column(incrementMetric(slam1)))

    df1.collect()
    assert(withRetries || slam1.value === 10)
    assert(slam1.getHighestRDDId.isDefined)
    val df1HighestId = slam1.getHighestRDDId.get
    val df1ExecutedPlanRddId = df1.queryExecution.executedPlan.execute().id
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10))
    assert(slam1.lastAttemptValueForRDDId(df1HighestId) === Some(10))
    // Values retrieved from the Dataset are the same as from the RDD.
    assert(slam1.lastAttemptValueForDataset(df1) === Some(10))
    assert(slam1.lastAttemptValueForQueryExecution(df1.queryExecution) === Some(10))

    df1.collect()
    assert(withRetries || slam1.value === 20) // +10
    // The same executedPlan RDD is reused, but getByteArrayRdd creates a new wrapper.
    assert(df1.queryExecution.executedPlan.execute().id === df1ExecutedPlanRddId)
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10))
    // Both wrapper RDDs are summed in allRDDs.
    assert(slam1.lastAttemptValueForAllRDDs() === Some(20))
    assert(slam1.lastAttemptValueForDataset(df1) === Some(10))

    val df2 = df1.filter("id < 5").filter(Column(incrementMetric(slam1)))
    df2.collect()
    assert(withRetries || slam1.value === 35) // +15
    assert(slam1.getHighestRDDId.isDefined)
    // Both incrementMetric expressions are within the same Stage, so they record together.
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(15))
    // allRDDs includes wrapper RDDs from repeated df1.collect() calls.
    assert(slam1.lastAttemptValueForAllRDDs() === Some(35))
    // New Dataset records only new value.
    assert(slam1.lastAttemptValueForDataset(df2) === Some(15))
    // Value df1 is still remembered.
    assert(slam1.lastAttemptValueForDataset(df1) === Some(10))

    val slam2 = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM2")
    val df3 = df1.repartition(1).filter(Column(incrementMetric(slam2)))
    df3.collect()
    assert(withRetries || slam1.value === 45) // +10
    assert(withRetries || slam2.value === 10) // new
    // df3 creates a new plan, and the plan / RDD from df1 is not reused.
    assert(slam1.getHighestRDDId.isDefined)
    val slam1HighestId = slam1.getHighestRDDId.get
    assert(slam1HighestId != df1HighestId) // new plan, new RDD id
    assert(slam1.lastAttemptValueForHighestRDDId() === Some(10)) // from new execution
    assert(slam1.lastAttemptValueForRDDId(df1HighestId) === Some(10)) // from first exec of df1
    // allRDDs includes wrapper RDDs from repeated collects.
    assert(slam1.lastAttemptValueForAllRDDs() === Some(45))
    assert(slam2.lastAttemptValueForAllRDDs() === Some(10))
    // slam1 and slam2 are both executed in df3
    assert(slam1.lastAttemptValueForDataset(df3) === Some(10))
    assert(slam2.lastAttemptValueForDataset(df3) === Some(10))
    // slam2 is not executed in df1 and df2.
    assert(slam2.lastAttemptValueForDataset(df1) === Some(0))
    assert(slam2.lastAttemptValueForDataset(df2) === Some(0))
    // slam1 value from df1 and df2 are still remembered.
    assert(slam1.lastAttemptValueForDataset(df1) === Some(10))
    assert(slam1.lastAttemptValueForDataset(df2) === Some(15))

    // Plans and RDDs get reused (result stage is re-executed; shuffle stage is purely reused).
    df3.collect()
    // No change in dataset values.
    assert(slam1.lastAttemptValueForDataset(df3) === Some(10))
    assert(slam2.lastAttemptValueForDataset(df3) === Some(10))
    assert(slam1.lastAttemptValueForDataset(df1) === Some(10))
    assert(slam1.lastAttemptValueForDataset(df2) === Some(15))

    logInfo(slam1.logAccumulatorState)
    logInfo(slam2.logAccumulatorState)
  }

  test("dataset limit") {
    val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM1")
    // 10 partitions of 10 elements each
    val df = spark.range(0, 1000, 1, 10).filter(Column(incrementMetric(slam)))
    var expectedMetricValue = 0
    var expectedSLAMValue = 0

    // Note: this is sensitive to the internal implementation of LimitExec.

    df.take(5)
    // One partition executed, local limit pushed into partition.
    expectedMetricValue = 5
    expectedSLAMValue = 5
    assert(withRetries || slam.value === expectedMetricValue)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(expectedSLAMValue))
    // take(5) actually inline creates a new Dataset, with new executed plan
    assert(slam.lastAttemptValueForDataset(df) === Some(0))

    df.take(50)
    // One partition executed, local limit pushed into partition.
    expectedMetricValue += 50
    expectedSLAMValue = 50
    assert(withRetries || slam.value === expectedMetricValue)
    // New SQL plan creates new RDDs, so this is seen as new execution.
    assert(slam.lastAttemptValueForHighestRDDId() === Some(expectedSLAMValue))
    assert(slam.getNumRDDs === 2)
    assert(slam.lastAttemptValueForAllRDDs() === Some(expectedMetricValue))
    // take(50) executes a different inline Dataset and plan.
    assert(slam.lastAttemptValueForDataset(df) === Some(0))

    df.take(220)
    // Three partitions executed.
    expectedMetricValue += 300
    expectedSLAMValue = 300
    assert(withRetries || slam.value === expectedMetricValue)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(expectedSLAMValue))
    assert(slam.getNumRDDs === 3)
    assert(slam.lastAttemptValueForAllRDDs() === Some(expectedMetricValue))
    // take(220) executes a different inline Dataset and plan.
    assert(slam.lastAttemptValueForDataset(df) === Some(0))

    df.take(320)
    // Five partitions executed.
    expectedMetricValue += 500
    expectedSLAMValue = 500
    assert(withRetries || slam.value === expectedMetricValue)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(expectedSLAMValue))
    assert(slam.getNumRDDs === 4)
    assert(slam.lastAttemptValueForAllRDDs() === Some(expectedMetricValue))
    // take(320) executes a different inline Dataset and plan.
    assert(slam.lastAttemptValueForDataset(df) === Some(0))

    df.take(1)
    // One partition scanned, local limit pushed into partition.
    expectedMetricValue += 1
    expectedSLAMValue = 1
    assert(withRetries || slam.value === expectedMetricValue)
    // New RDD, so the value from new execution is back to 1.
    assert(slam.lastAttemptValueForHighestRDDId() === Some(expectedSLAMValue))
    assert(slam.getNumRDDs === 5)
    assert(slam.lastAttemptValueForAllRDDs() === Some(expectedMetricValue))
    // take(1) executes a different inline Dataset and plan.
    assert(slam.lastAttemptValueForDataset(df) === Some(0))

    logInfo(slam.logAccumulatorState)
  }

  test("driver set value") {
    val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
    slam.set(10)

    // Regular metric value
    assert(withRetries || slam.value === 10)

    assert(slam.getDirectDriverValue === Some(10))
    // "Driver update" is returned under "highest" and "all" RDDs
    assert(slam.lastAttemptValueForHighestRDDId() === Some(10))
    assert(slam.lastAttemptValueForAllRDDs() === Some(10))
    assert(slam.getNumRDDs === 0)
    // When specific RDDs are requested, driver value is not returned.
    assert(slam.lastAttemptValueForRDDId(42) === Some(0))
    assert(slam.lastAttemptValueForRDDIds(Seq(7, 42)) === Some(0))

    // Incrementing works
    slam.add(5)
    assert(withRetries || slam.value === 15)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(15))
    assert(slam.lastAttemptValueForAllRDDs() === Some(15))
    assert(slam.getDirectDriverValue === Some(15))

    // Negative increments are ignored by SQLMetric
    slam.add(-3)
    assert(withRetries || slam.value === 15)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(15))
    assert(slam.lastAttemptValueForAllRDDs() === Some(15))
    assert(slam.getDirectDriverValue === Some(15))

    // Reset does not reset SLAM.
    slam.reset()
    assert(withRetries || slam.value === 0)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(15))
    assert(slam.lastAttemptValueForAllRDDs() === Some(15))
    assert(slam.getDirectDriverValue === Some(15))

    // Setting it back...
    slam.set(20)
    assert(withRetries || slam.value === 20)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(20))
    assert(slam.lastAttemptValueForAllRDDs() === Some(20))
    assert(slam.getDirectDriverValue === Some(20))
    assert(slam.getNumRDDs === 0)

    val df = spark.range(10).filter(Column(incrementMetric(slam)))
    // SLAM was not executed in this Dataset, the driver value set manually
    // before should not be returned.
    assert(slam.lastAttemptValueForDataset(df) === Some(0))
    assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === None)
    df.collect()
    assert(withRetries || slam.value === 30)
    // SLAM bails out when it sees both driver and executor values
    assert(slam.lastAttemptValueForHighestRDDId() === None)
    assert(slam.lastAttemptValueForAllRDDs() === None)
    assert(slam.lastAttemptValueForRDDId(42) === None)
    assert(slam.lastAttemptValueForRDDIds(Seq(7, 42)) === None)
    assert(slam.lastAttemptValueForDataset(df) === None)
    assert(!slam.getValid)
    assert(slam.getNumRDDs === 0) // invalidated before RDD got recorded
    assert(slam.getDirectDriverValue === Some(20))
    // Invalidated before QueryExecution value was recorded.
    assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === None)

    slam.reset()
    slam.set(10)
    assert(withRetries || slam.value === 10)
    // SLAM stays bailed out.
    assert(slam.lastAttemptValueForHighestRDDId() === None)
    assert(slam.lastAttemptValueForAllRDDs() === None)
    assert(slam.lastAttemptValueForRDDId(42) === None)
    assert(slam.lastAttemptValueForRDDIds(Seq(7, 42)) === None)
    assert(slam.lastAttemptValueForDataset(df) === None)
    assert(!slam.getValid)
    // SLAM info doesn't get updated anymore when invalid, but stays around for debugging purposes.
    assert(slam.getNumRDDs === 0)
    assert(slam.getDirectDriverValue === Some(20))
    assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === None)

    logInfo(slam.logAccumulatorState)

    // resetLastAttemptAccumulator resets it and makes it valid to be used again.
    slam.resetLastAttemptAccumulator()
    assert(slam.getValid)
    slam.set(42)
    assert(slam.lastAttemptValueForHighestRDDId() === Some(42))
    assert(slam.getDirectDriverValue === Some(42))
    assert(slam.getNumRDDs === 0)
    assert(slam.lastAttemptValueForDataset(df) === Some(0))
  }

  test("ConvertToLocalRelation direct driver execution") {
    // Normally ConvertToLocalRelation is disabled in tests.
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
      val df = Seq(1, 2, 3).toDF("a").filter(Column(incrementMetric(slam)))

      // SLAM is executed on the driver in the Optimized by ConvertToLocalRelation
      df.collect()
      assert(slam.lastAttemptValueForAllRDDs() === Some(3))
      assert(slam.lastAttemptValueForHighestRDDId() === Some(3))
      assert(slam.getDirectDriverValue === Some(3))
      // SLAM recognizes it was executed on the driver
      // in the scope of the QueryExecution of this Dataset.
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === Some(3))

      // Second action does not re-execute Optimizer.
      df.collect()
      assert(slam.lastAttemptValueForAllRDDs() === Some(3))
      assert(slam.lastAttemptValueForHighestRDDId() === Some(3))
      assert(slam.getDirectDriverValue === Some(3))
      assert(slam.lastAttemptValueForDataset(df) === Some(3))

      // Limitation: When a new Dataset is built and Optimizer reexecutes ConvertToLocalRelation,
      // SLAM RDD retrieval cannot reason about re-execution on the driver,
      // leading to duplicated metrics.
      val df2 = df.withColumn("foo", Column(Literal("foo")))
      df2.collect()
      assert(slam.lastAttemptValueForAllRDDs() === Some(6))
      assert(slam.lastAttemptValueForHighestRDDId() === Some(6))
      assert(slam.getDirectDriverValue === Some(6))
      // But it recognizes that it is done in a new QueryExecution and is able to distinguish that
      // without duplicates.
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      assert(slam.lastAttemptValueForDataset(df2) === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df2.queryExecution.id.toString) === Some(3))

      // No RDD executions were recorded.
      assert(slam.getNumRDDs === 0)

      logInfo(slam.logAccumulatorState)
    }
  }

  test("ConvertToLocalRelation manual optimizer triggering") {
    // Normally ConvertToLocalRelation is disabled in tests.
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
      val df = Seq(1, 2, 3).toDF("a").filter(Column(incrementMetric(slam)))
      // Trigger the optimizer manually, which will trigger ConvertToLocalRelation
      df.queryExecution.assertOptimized()

      // SLAM recognizes it was executed on the driver
      // in the scope of the QueryExecution of this Dataset.
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === Some(3))

      // Repeated actions do not re-execute Optimizer.
      df.collect()
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      df.collect()
      assert(slam.lastAttemptValueForDataset(df) === Some(3))

      logInfo(slam.logAccumulatorState)
    }
  }

  test("ConvertToLocalRelation in explain") {
    // Normally ConvertToLocalRelation is disabled in tests.
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
      val df = Seq(1, 2, 3).toDF("a").filter(Column(incrementMetric(slam)))

      // EXPLAIN triggers the optimizer and triggered ConvertToLocalRelation to execute
      df.explain(true)
      assert(withRetries || slam.value === 3)
      assert(slam.lastAttemptValueForAllRDDs() === Some(3))
      assert(slam.lastAttemptValueForHighestRDDId() === Some(3))
      assert(slam.getDirectDriverValue === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === Some(3))
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      // Retriggering EXPLAIN does not cause duplicates
      df.explain(true)
      assert(withRetries || slam.value === 3)
      assert(slam.lastAttemptValueForAllRDDs() === Some(3))
      assert(slam.lastAttemptValueForHighestRDDId() === Some(3))
      assert(slam.getDirectDriverValue === Some(3))
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === Some(3))

      // Execution does not re-execute Optimizer and does not duplicate metric.
      df.collect()
      assert(withRetries || slam.value === 3)
      assert(slam.lastAttemptValueForAllRDDs() === Some(3))
      assert(slam.lastAttemptValueForHighestRDDId() === Some(3))
      assert(slam.getDirectDriverValue === Some(3))
      assert(slam.lastAttemptValueForDataset(df) === Some(3))
      assert(slam.getDirectDriverQueryExecutionValue(df.queryExecution.id.toString) === Some(3))

      // No RDD executions were recorded.
      assert(slam.getNumRDDs === 0)

      logInfo(slam.logAccumulatorState)
    }
  }

  test("BroadcastNestedLoopJoin outer executes probe side twice") {
    val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
    val build =
      spark.range(5).selectExpr("id as b").hint("broadcast")
    val probe = spark.range(100).selectExpr("id as p").filter(Column(incrementMetric(slam)))
    val df = probe.join(build, usingColumns = Seq(), joinType = "rightouter")
    df.collect()
    assert(AdaptiveSparkPlanHelper.exists(df.queryExecution.executedPlan) {
      case BroadcastNestedLoopJoinExec(_, _, BuildRight, RightOuter, None) => true
      case _ => false
    })
    // When build side is outer, probe side gets executed twice by BNLJ:
    // once for matches, and once to mark unmatched build rows.
    // This is a non-determinism correctness issue, and the two executions
    // should not be double-counted in the last attempt value.
    assert(slam.getNumRDDs === 2)
    assert(slam.lastAttemptValueForAllRDDs() === Some(200))
    // The two executions are different RDDs, but only one of them is highest id.
    assert(slam.lastAttemptValueForHighestRDDId() === Some(100))
    // Dataset dedups per scope and returns only the latest RDD's value.
    assert(slam.lastAttemptValueForDataset(df) === Some(100))
  }

  test("SLAM with AQE CoalesceShufflePartitions") {
    // Adapted from tests in CoalesceShufflePartitionsSuite

    val stage1Slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM2")
    val stage2Slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "SLAM2")
    val stage1MetricExpr = Column(incrementMetric(stage1Slam))
    val stage2MetricExpr = Column(incrementMetric(stage2Slam))

    // Dataframe with a SLAM before and after a shuffle.
    val df = spark.range(0, 1000, 1, numPartitions = 10)
      .selectExpr("id % 20 as key", "id as value")
      .filter(stage1MetricExpr)
      .groupBy("key").count()
      .filter(stage2MetricExpr)

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "2000") {
      df.collect()
    }

    // Verify the AQE coalescing happened and coalesced the shuffle into 3 partitions.
    // (based on ADVISORY_PARTITION_SIZE_IN_BYTES config)
    val finalPlan = AdaptiveSparkPlanHelper.stripAQEPlan(df.queryExecution.executedPlan)
    val shuffleReads = finalPlan.collect {
      case r @ CoalescedShuffleRead() => r
    }
    assert(shuffleReads.nonEmpty)
    shuffleReads.foreach { read =>
      // check there is actual coalescing of partitions happening
      assert(read.isCoalescedRead)
      assert(read.partitionSpecs.exists {
        case p: CoalescedPartitionSpec if p.startReducerIndex < p.endReducerIndex - 1 => true
        case _ => false
      })
    }

    // Verify SLAM metrics.
    assert(stage1Slam.lastAttemptValueForHighestRDDId() === Some(1000))
    assert(stage2Slam.lastAttemptValueForHighestRDDId() === Some(20))
    assert(stage1Slam.lastAttemptValueForDataset(df) === Some(1000))
    assert(stage2Slam.lastAttemptValueForDataset(df) === Some(20))
  }

  test("WholeStageCodegenExec fallback to non-codegen") {
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "1" // force fallback due to too large method
    ) {
      // This test is to verify that SLAM works correctly when WholeStageCodegenExec falls back
      // to non-codegen execution.
      val slam = SQLLastAttemptMetrics.createMetric(spark.sparkContext, "test SLAM")
      val df = spark
        .range(10)
        .filter(Column(incrementMetric(slam)))
        // these two operators will be turned into a WholeStageCodegen,
        .selectExpr("id + 1 as foo", "id + 2 as bar")
        .filter("foo < bar")
      df.collect()
      assert(slam.lastAttemptValueForDataset(df) === Some(10))
      // Metric is attributed to the child of the WSCG node.
      val wscg = df.queryExecution.executedPlan.collectFirst {
        case w: WholeStageCodegenExec => w
      }
      assert(wscg.isDefined)
      assert(slam.getHighestRDDId.isDefined)
    }
  }
}

class SQLLastAttemptMetricIntegrationSuiteWithStageRetries
    extends SQLLastAttemptMetricIntegrationSuite {
  override protected def withRetries = true

  override protected def test(
      testName: String,
      testTags: org.scalatest.Tag*)
      (testFun: => Any)
      (implicit pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags : _*) {
      withSparkContextConf(config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true") {
        // Stage retries should not affect SLAM metrics.
        testFun
      }
    }(pos)
  }
}
