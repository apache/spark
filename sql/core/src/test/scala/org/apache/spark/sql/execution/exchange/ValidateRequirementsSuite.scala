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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, SinglePartition}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.test.SharedSparkSession

class ValidateRequirementsSuite extends PlanTest with SharedSparkSession {

  import testImplicits._

  private def testValidate(
      joinKeyIndices: Seq[Int],
      leftPartitionKeyIndices: Seq[Int],
      rightPartitionKeyIndices: Seq[Int],
      leftPartitionNum: Int,
      rightPartitionNum: Int,
      success: Boolean): Unit = {
    val table1 =
      spark.range(10).select($"id" + 1 as Symbol("a1"), $"id" + 2 as Symbol("b1"),
        $"id" + 3 as Symbol("c1")).queryExecution.executedPlan
    val table2 =
      spark.range(10).select($"id" + 1 as Symbol("a2"), $"id" + 2 as Symbol("b2"),
        $"id" + 3 as Symbol("c2")).queryExecution.executedPlan

    val leftKeys = joinKeyIndices.map(table1.output)
    val rightKeys = joinKeyIndices.map(table2.output)
    val leftPartitioning =
      HashPartitioning(leftPartitionKeyIndices.map(table1.output), leftPartitionNum)
    val rightPartitioning =
      HashPartitioning(rightPartitionKeyIndices.map(table2.output), rightPartitionNum)
    val left =
      SortExec(leftKeys.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(leftPartitioning, table1))
    val right =
      SortExec(rightKeys.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(rightPartitioning, table2))

    val plan = SortMergeJoinExec(leftKeys, rightKeys, Inner, None, left, right)
    assert(ValidateRequirements.validate(plan) == success, plan)
  }

  test("SMJ requirements satisfied with partial partition key") {
    testValidate(Seq(0, 1, 2), Seq(1), Seq(1), 5, 5, true)
  }

  test("SMJ requirements satisfied with different partition key order") {
    testValidate(Seq(0, 1, 2), Seq(2, 0, 1), Seq(2, 0, 1), 5, 5, true)
  }

  test("SMJ requirements not satisfied with unequal partition key order") {
    testValidate(Seq(0, 1, 2), Seq(1, 0), Seq(0, 1), 5, 5, false)
  }

  test("SMJ requirements not satisfied with unequal partition key length") {
    testValidate(Seq(0, 1, 2), Seq(1), Seq(1, 2), 5, 5, false)
  }

  test("SMJ requirements not satisfied with partition key missing from join key") {
    testValidate(Seq(1, 2), Seq(1, 0), Seq(1, 0), 5, 5, false)
  }

  test("SMJ requirements not satisfied with unequal partition number") {
    testValidate(Seq(0, 1, 2), Seq(0, 1, 2), Seq(0, 1, 2), 12, 10, false)
  }

  test("SMJ with HashPartitioning(1) and SinglePartition") {
    val table1 = spark.range(10).queryExecution.executedPlan
    val table2 = spark.range(10).queryExecution.executedPlan
    val leftPartitioning = HashPartitioning(table1.output, 1)
    val rightPartitioning = SinglePartition
    val left =
      SortExec(table1.output.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(leftPartitioning, table1))
    val right =
      SortExec(table2.output.map(SortOrder(_, Ascending)), false,
        ShuffleExchangeExec(rightPartitioning, table2))

    val plan = SortMergeJoinExec(table1.output, table2.output, Inner, None, left, right)
    assert(ValidateRequirements.validate(plan), plan)
  }

  private def testNestedJoin(
      joinKeyIndices1: Seq[(Int, Int)],
      joinKeyIndices2: Seq[(Int, Int)],
      partNums: Seq[Int],
      success: Boolean): Unit = {
    val table1 =
      spark.range(10).select($"id" + 1 as Symbol("a1"), $"id" + 2 as Symbol("b1"),
        $"id" + 3 as Symbol("c1")).queryExecution.executedPlan
    val table2 =
      spark.range(10).select($"id" + 1 as Symbol("a2"), $"id" + 2 as Symbol("b2"),
        $"id" + 3 as Symbol("c2")).queryExecution.executedPlan
    val table3 =
      spark.range(10).select($"id" + 1 as Symbol("a3"), $"id" + 2 as Symbol("b3"),
        $"id" + 3 as Symbol("c3")).queryExecution.executedPlan

    val key1 = joinKeyIndices1.map(_._1).map(table1.output)
    val key2 = joinKeyIndices1.map(_._2).map(table2.output)
    val key3 = joinKeyIndices2.map(_._1).map(table3.output)
    val key4 = joinKeyIndices2.map(_._2).map(table1.output ++ table2.output)
    val partitioning1 = HashPartitioning(key1, partNums(0))
    val partitioning2 = HashPartitioning(key2, partNums(1))
    val partitioning3 = HashPartitioning(key3, partNums(2))
    val joinRel1 =
      SortExec(key1.map(SortOrder(_, Ascending)), false, ShuffleExchangeExec(partitioning1, table1))
    val joinRel2 =
      SortExec(key2.map(SortOrder(_, Ascending)), false, ShuffleExchangeExec(partitioning2, table2))
    val joinRel3 =
      SortExec(key3.map(SortOrder(_, Ascending)), false, ShuffleExchangeExec(partitioning3, table3))

    val plan = SortMergeJoinExec(key3, key4, Inner, None,
      joinRel3, SortMergeJoinExec(key1, key2, Inner, None, joinRel1, joinRel2))
    assert(ValidateRequirements.validate(plan) == success, plan)
  }

  test("ValidateRequirements should work bottom up") {
    Seq(true, false).foreach { success =>
      testNestedJoin(Seq((0, 0)), Seq((0, 0)), Seq(5, if (success) 5 else 10, 5), success)
    }
  }

  test("PartitioningCollection exact match") {
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((0, 0), (1, 1)), Seq(5, 5, 5), true)
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((0, 3), (1, 4)), Seq(5, 5, 5), true)
  }

  test("PartitioningCollection mismatch with different order") {
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((1, 1), (0, 0)), Seq(5, 5, 5), false)
    testNestedJoin(Seq((0, 0), (1, 1)), Seq((1, 4), (0, 3)), Seq(5, 5, 5), false)
  }

  test("PartitioningCollection mismatch with different set") {
    testNestedJoin(Seq((1, 1)), Seq((2, 2), (1, 1)), Seq(5, 5, 5), false)
    testNestedJoin(Seq((1, 1)), Seq((2, 5), (1, 4)), Seq(5, 5, 5), false)
  }

  test("PartitioningCollection mismatch with key missing from required") {
    testNestedJoin(Seq((2, 2), (1, 1)), Seq((2, 2)), Seq(5, 5, 5), false)
    testNestedJoin(Seq((2, 2), (1, 1)), Seq((2, 5)), Seq(5, 5, 5), false)
  }
}
