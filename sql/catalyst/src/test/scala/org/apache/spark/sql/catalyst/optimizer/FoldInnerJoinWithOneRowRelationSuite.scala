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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class FoldInnerJoinWithOneRowRelationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("FoldInnerJoinWithOneRowRelation", FixedPoint(10),
        FoldInnerJoinWithOneRowRelation) :: Nil
  }

  private val tbl = LocalRelation($"a".int, $"b".int)

  test("SPARK-57039: fold Inner join with OneRowRelation on the right (no condition)") {
    val plan = tbl.join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, tbl.analyze)
  }

  test("SPARK-57039: fold Inner join with OneRowRelation on the left (no condition)") {
    val plan = OneRowRelation().join(tbl, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, tbl.analyze)
  }

  test("SPARK-57039: fold Inner join with OneRowRelation and a condition into a Filter") {
    val plan = tbl.join(OneRowRelation(), Inner, Some($"a" > 5)).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, tbl.where($"a" > 5).analyze)
  }

  test("SPARK-57039: do NOT fold non-Inner join with OneRowRelation") {
    val plan = tbl.join(OneRowRelation(), LeftOuter, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("SPARK-57039: fold join with ArrayType column on the kept side") {
    val arrTbl = LocalRelation(Symbol("a").array(IntegerType), Symbol("b").int)
    val plan = arrTbl.join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, arrTbl.analyze)
  }

  test("SPARK-57039: fold join with MapType column on the kept side") {
    val mapTbl = LocalRelation(Symbol("m").map(StringType, IntegerType), Symbol("b").int)
    val plan = mapTbl.join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, mapTbl.analyze)
  }

  test("SPARK-57039: fold join with StructType column on the kept side") {
    import org.apache.spark.sql.catalyst.expressions.AttributeReference
    val sAttr = AttributeReference("s",
      StructType(StructField("x", IntegerType) :: StructField("y", StringType) :: Nil))()
    val structTbl = LocalRelation(sAttr, Symbol("b").int)
    val plan = structTbl.join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, structTbl.analyze)
  }

  test("SPARK-57039: nested fold - Join(Join(tbl, OneRow), OneRow) collapses to tbl") {
    val plan = tbl.join(OneRowRelation(), Inner, None).join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, tbl.analyze)
  }

  test("SPARK-57039: do NOT fold when condition contains an unevaluable expression") {
    import org.apache.spark.sql.catalyst.expressions.ScalarSubquery
    val subq = ScalarSubquery(tbl.select(Symbol("a")).analyze)
    val plan = tbl.join(OneRowRelation(), Inner, Some(Symbol("a") === subq)).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("SPARK-57039: do NOT fold when both sides are OneRowRelation (cartesian product)") {
    val plan = OneRowRelation().join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("SPARK-57039: do NOT fold when OneRowRelation joins a single-row LocalRelation") {
    val singleRow = LocalRelation.fromExternalRows(
      Seq(Symbol("x").int, Symbol("y").string), Seq(Row(1, "a")))
    val plan = singleRow.join(OneRowRelation(), Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }
}
