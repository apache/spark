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

import org.apache.spark.sql.catalyst.expressions.{Add, Attribute}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

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
    assert(outputOrdering.head.sameOrderExpressions.size == 0)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - partitioning - multi-references") {
    val df = spark.range(2).selectExpr("id as a", "id as b")
      .repartition($"a" + $"b").selectExpr("a as x", "b as y")
    val outputPartitioning = stripAQEPlan(df.queryExecution.executedPlan).outputPartitioning
    // (a + b), (a + y), (x + b) are pruned since their references are not the subset of output
    outputPartitioning match {
      case HashPartitioning(Seq(Add(l: Attribute, r: Attribute, _)), _) =>
        assert(l.name == "x" && r.name == "y")
      case _ => fail(s"Unexpected $outputPartitioning")
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to complex " +
    "expressions") {
    val df2 = spark.range(2).repartition($"id" + $"id").selectExpr("id + id as a", "id + id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    assert(partitionings.size == 2)

    val df = spark.range(2).orderBy($"id" + $"id").selectExpr("id + id as a", "id + id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.children.size == 2)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to children of " +
    "complex expressions") {
    val df2 = spark.range(2).repartition($"id" + $"id").selectExpr("id as a", "id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    // (a + b) is the very same as (b + a) so keep only one
    assert(partitionings.size == 3)

    val df = spark.range(2).orderBy($"id" + $"id").selectExpr("id as a", "id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    // (a + b) is the very same as (b + a) so keep only one
    assert(outputOrdering.head.children.size == 3)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to complex " +
    "expressions and to their children") {
    val df2 = spark.range(2).repartition($"id" + $"id")
      .selectExpr("id + id as aa", "id + id as bb", "id as a", "id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    assert(partitionings.size == 5)

    val df = spark.range(2).orderBy($"id" + $"id")
      .selectExpr("id + id as aa", "id + id as bb", "id as a", "id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.children.size == 5)
  }
}
