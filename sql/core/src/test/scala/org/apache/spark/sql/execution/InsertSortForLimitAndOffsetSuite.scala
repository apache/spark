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

import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.IntegratedUDFTestUtils._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class InsertSortForLimitAndOffsetSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def assertHasTopKSort(plan: SparkPlan): Unit = {
    assert(find(plan) {
      case _: TakeOrderedAndProjectExec => true
      case _ => false
    }.isDefined)
  }

  private def assertHasCollectLimitExec(plan: SparkPlan): Unit = {
    assert(find(plan) {
      case _: CollectLimitExec => true
      case _ => false
    }.isDefined)
  }

  private def assertHasGlobalLimitExec(plan: SparkPlan): Unit = {
    assert(find(plan) {
      case _: GlobalLimitExec => true
      case _ => false
    }.isDefined)
  }

  private def hasLocalSort(plan: SparkPlan): Boolean = {
    find(plan) {
      case GlobalLimitExec(_, s: SortExec, _) => !s.global
      case GlobalLimitExec(_, ProjectExec(_, s: SortExec), _) => !s.global
      case _ => false
    }.isDefined
  }

  test("root LIMIT preserves data ordering with top-K sort") {
    val df = spark.range(10).orderBy($"id" % 8).limit(2)
    df.collect()
    val physicalPlan = df.queryExecution.executedPlan
    assertHasTopKSort(physicalPlan)
    // Extra local sort is not needed for LIMIT with top-K sort optimization.
    assert(!hasLocalSort(physicalPlan))
  }

  test("middle LIMIT preserves data ordering with top-K sort") {
    val df = spark.range(10).orderBy($"id" % 8).limit(2).distinct()
    df.collect()
    val physicalPlan = df.queryExecution.executedPlan
    assertHasTopKSort(physicalPlan)
    // Extra local sort is not needed for LIMIT with top-K sort optimization.
    assert(!hasLocalSort(physicalPlan))
  }

  test("root LIMIT preserves data ordering with CollectLimitExec") {
    withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1") {
      val df = spark.range(10).orderBy($"id" % 8).limit(2)
      df.collect()
      val physicalPlan = df.queryExecution.executedPlan
      assertHasCollectLimitExec(physicalPlan)
      // Extra local sort is not needed for root LIMIT
      assert(!hasLocalSort(physicalPlan))
    }
  }

  test("middle LIMIT preserves data ordering with the extra sort") {
    withSQLConf(
      SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1",
      // To trigger the bug, we have to disable the coalescing optimization. Otherwise we use only
      // one partition to read the range-partition shuffle and there is only one shuffle block for
      // the final single-partition shuffle, random fetch order is no longer an issue.
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val df = 1.to(10).map(v => v -> v).toDF("c1", "c2").orderBy($"c1" % 8)
      verifySortAdded(df.limit(2))
      verifySortAdded(df.filter($"c2" > rand()).limit(2))
      verifySortAdded(df.select($"c2").limit(2))
      verifySortAdded(df.filter($"c2" > rand()).select($"c2").limit(2))

      assume(shouldTestPythonUDFs)
      val pythonTestUDF = TestPythonUDF(name = "pyUDF", Some(IntegerType))
      verifySortAdded(df.filter(pythonTestUDF($"c2") > rand()).limit(2))
      verifySortAdded(df.select(pythonTestUDF($"c2")).limit(2))
    }
  }

  test("root OFFSET preserves data ordering with CollectLimitExec") {
    val df = spark.range(10).orderBy($"id" % 8).offset(2)
    df.collect()
    val physicalPlan = df.queryExecution.executedPlan
    assertHasCollectLimitExec(physicalPlan)
    // Extra local sort is not needed for root OFFSET
    assert(!hasLocalSort(physicalPlan))
  }

  test("middle OFFSET preserves data ordering with the extra sort") {
    val df = 1.to(10).map(v => v -> v).toDF("c1", "c2").orderBy($"c1" % 8)
    verifySortAdded(df.offset(2))
    verifySortAdded(df.filter($"c2" > rand()).offset(2))
    verifySortAdded(df.select($"c2").offset(2))
    verifySortAdded(df.filter($"c2" > rand()).select($"c2").offset(2))

    assume(shouldTestPythonUDFs)
    val pythonTestUDF = TestPythonUDF(name = "pyUDF", Some(IntegerType))
    verifySortAdded(df.filter(pythonTestUDF($"c2") > rand()).offset(2))
    verifySortAdded(df.select(pythonTestUDF($"c2")).offset(2))
  }

  private def verifySortAdded(df: Dataset[_]): Unit = {
    // Do distinct to trigger a shuffle, so that the LIMIT/OFFSET below won't be planned as
    // `CollectLimitExec`
    val shuffled = df.distinct()
    shuffled.collect()
    val physicalPlan = shuffled.queryExecution.executedPlan
    assertHasGlobalLimitExec(physicalPlan)
    // Extra local sort is needed for middle LIMIT/OFFSET
    assert(hasLocalSort(physicalPlan))
    // Make sure the schema does not change.
    assert(physicalPlan.schema == shuffled.schema)
  }
}
