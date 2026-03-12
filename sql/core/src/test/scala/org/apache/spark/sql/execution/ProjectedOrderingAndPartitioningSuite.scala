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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class ProjectedOrderingAndPartitioningSuite
  extends SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("SPARK-42049: Improve AliasAwareOutputExpression - ordering - multi-alias") {
    Seq(0, 1, 2, 5).foreach { limit =>
      withSQLConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT.key -> limit.toString) {
        val df = spark.range(2).orderBy($"id").selectExpr("id as x", "id as y", "id as z")
        val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
        limit match {
          case 5 =>
            assert(outputOrdering.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.size == 2)
            assert(outputOrdering.head.sameOrderExpressions.map(_.asInstanceOf[Attribute].name)
              .toSet.subsetOf(Set("x", "y", "z")))
          case 2 =>
            assert(outputOrdering.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.map(_.asInstanceOf[Attribute].name)
              .toSet.subsetOf(Set("x", "y", "z")))
          case 1 =>
            assert(outputOrdering.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.size == 0)
          case 0 =>
            assert(outputOrdering.size == 0)
        }
      }
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - partitioning - multi-alias") {
    Seq(0, 1, 2, 5).foreach { limit =>
      withSQLConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT.key -> limit.toString) {
        val df = spark.range(2).repartition($"id").selectExpr("id as x", "id as y", "id as z")
        val outputPartitioning = stripAQEPlan(df.queryExecution.executedPlan).outputPartitioning
        limit match {
          case 5 =>
            val p = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
            assert(p.size == 3)
            assert(p.flatMap(_.asInstanceOf[HashPartitioning].expressions
              .map(_.asInstanceOf[Attribute].name)).toSet == Set("x", "y", "z"))
          case 2 =>
            val p = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
            assert(p.size == 2)
            p.flatMap(_.asInstanceOf[HashPartitioning].expressions
              .map(_.asInstanceOf[Attribute].name)).toSet.subsetOf(Set("x", "y", "z"))
          case 1 =>
            val p = outputPartitioning.asInstanceOf[HashPartitioning]
            assert(p.expressions.size == 1)
            assert(p.expressions.map(_.asInstanceOf[Attribute].name)
              .toSet.subsetOf(Set("x", "y", "z")))
          case 0 =>
            assert(outputPartitioning.isInstanceOf[UnknownPartitioning])
        }
      }
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - ordering - multi-references") {
    val df = spark.range(2).selectExpr("id as a", "id as b")
      .orderBy($"a" + $"b").selectExpr("a as x", "b as y")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "(x + y) ASC NULLS FIRST")
    assert(outputOrdering.head.sameOrderExpressions.size == 0)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - partitioning - multi-references") {
    val df = spark.range(2).selectExpr("id as a", "id as b")
      .repartition($"a" + $"b").selectExpr("a as x", "b as y")
    val outputPartitioning = stripAQEPlan(df.queryExecution.executedPlan).outputPartitioning
    // (a + b), (a + y), (x + b) are pruned since their references are not the subset of output
    outputPartitioning match {
      case p: HashPartitioning => assert(p.sql == "hashpartitioning((x + y))")
      case _ => fail(s"Unexpected $outputPartitioning")
    }
  }

  test("SPARK-46609: Avoid exponential explosion in PartitioningPreservingUnaryExecNode") {
    withSQLConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT.key -> "2") {
      val output = Seq(AttributeReference("a", StringType)(), AttributeReference("b", StringType)())
      val plan = ProjectExec(
        Seq(
          Alias(output(0), "a1")(),
          Alias(output(0), "a2")(),
          Alias(output(1), "b1")(),
          Alias(output(1), "b2")()
        ),
        DummyLeafPlanExec(output)
      )
      assert(plan.outputPartitioning.asInstanceOf[PartitioningCollection].partitionings.length == 2)
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to complex " +
    "expressions") {
    val df2 = spark.range(2).repartition($"id" + $"id").selectExpr("id + id as a", "id + id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    assert(partitionings.map {
      case p: HashPartitioning => p.sql
      case _ => fail(s"Unexpected $outputPartitioning")
    } == Seq("hashpartitioning(b)", "hashpartitioning(a)"))

    val df = spark.range(2).orderBy($"id" + $"id").selectExpr("id + id as a", "id + id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "b ASC NULLS FIRST")
    assert(outputOrdering.head.sameOrderExpressions.map(_.sql) == Seq("a"))
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to children of " +
    "complex expressions") {
    val df2 = spark.range(2).repartition($"id" + $"id").selectExpr("id as a", "id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    // (a + b) is the same as (b + a) so expect only one
    assert(partitionings.map {
      case p: HashPartitioning => p.sql
      case _ => fail(s"Unexpected $outputPartitioning")
    } == Seq("hashpartitioning((b + b))", "hashpartitioning((a + b))", "hashpartitioning((a + a))"))

    val df = spark.range(2).orderBy($"id" + $"id").selectExpr("id as a", "id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "(b + b) ASC NULLS FIRST")
    // (a + b) is the same as (b + a) so expect only one
    assert(outputOrdering.head.sameOrderExpressions.map(_.sql) == Seq("(a + b)", "(a + a)"))
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to complex " +
    "expressions and to their children") {
    val df2 = spark.range(2).repartition($"id" + $"id")
      .selectExpr("id + id as aa", "id + id as bb", "id as a", "id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    // (a + b) is the same as (b + a) so expect only one
    assert(partitionings.map {
      case p: HashPartitioning => p.sql
      case _ => fail(s"Unexpected $outputPartitioning")
    } == Seq("hashpartitioning(bb)", "hashpartitioning(aa)", "hashpartitioning((b + b))",
      "hashpartitioning((a + b))", "hashpartitioning((a + a))"))

    val df = spark.range(2).orderBy($"id" + $"id")
      .selectExpr("id + id as aa", "id + id as bb", "id as a", "id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "bb ASC NULLS FIRST")
    // (a + b) is the same as (b + a) so expect only one
    assert(outputOrdering.head.sameOrderExpressions.map(_.sql) ==
      Seq("aa", "(b + b)", "(a + b)", "(a + a)"))
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - ordering partly projected") {
    val df = spark.range(2).orderBy($"id" + 1, $"id" + 2)

    val df1 = df.selectExpr("id + 1 AS a", "id + 2 AS b")
    val outputOrdering1 = df1.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering1.size == 2)
    assert(outputOrdering1.map(_.sql) == Seq("a ASC NULLS FIRST", "b ASC NULLS FIRST"))

    val df2 = df.selectExpr("id + 1 AS a")
    val outputOrdering2 = df2.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering2.size == 1)
    assert(outputOrdering2.head.sql == "a ASC NULLS FIRST")

    val df3 = df.selectExpr("id + 2 AS b")
    val outputOrdering3 = df3.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering3.size == 0)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - no alias but still prune expressions") {
    val df = spark.range(2).select($"id" + 1 as "a", $"id" + 2 as "b")

    val df1 = df.repartition($"a", $"b").selectExpr("a")
    val outputPartitioning = stripAQEPlan(df1.queryExecution.executedPlan).outputPartitioning
    assert(outputPartitioning.isInstanceOf[UnknownPartitioning])

    val df2 = df.orderBy("a", "b").select("a")
    val outputOrdering = df2.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.child.asInstanceOf[Attribute].name == "a")
    assert(outputOrdering.head.sameOrderExpressions.size == 0)
  }
}

private case class DummyLeafPlanExec(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = null
  override def outputPartitioning: Partitioning = {
    PartitioningCollection(output.map(attr => HashPartitioning(Seq(attr), 4)))
  }
}
