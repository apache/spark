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


package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ResolveRecursiveCTESuite extends AnalysisTest {
  // Recursive references must allow nested nulls without changing the anchor's schema.
  test("SPARK-60860: recursive reference output has nullable nested types") {
    val types = Seq(
      ArrayType(IntegerType, containsNull = false),
      StructType(Seq(StructField("x", ArrayType(IntegerType, containsNull = false), false))),
      MapType(
        StructType(Seq(StructField("key", IntegerType, false))),
        ArrayType(IntegerType, containsNull = false), valueContainsNull = false))
    for {
      dataType <- types
      columnNames <- Seq(None, Some(Seq("renamed")))
      legacy <- Seq(false, true)
    } {
      withSQLConf(
        SQLConf.LEGACY_RECURSIVE_CTE_USE_ANCHOR_NESTED_NULLABILITY.key -> legacy.toString) {
        val cteId = 0L
        val anchor = LocalRelation(AttributeReference("value", dataType, nullable = false)())
        val cteRef = CTERelationRef(cteId, _resolved = false, Nil, isStreaming = false)
        val recursion = cteRef.copy(recursive = true).subquery("t")
        val union = anchor.union(recursion)
        val definition = columnNames.map(UnresolvedSubqueryColumnAliases(_, union)).getOrElse(union)
        val plan = WithCTE(cteRef, Seq(CTERelationDef(definition.subquery("t"), cteId)))

        val analyzed = getAnalyzer.execute(plan)
        assert(analyzed.resolved)
        val loop = analyzed.collectFirst { case u: UnionLoop => u }.get
        val ref = loop.recursion.collectFirst { case r: UnionLoopRef => r }.get
        assert(loop.anchor.output.head.dataType == dataType)
        assert(!loop.anchor.output.head.nullable)
        val expectedType = if (legacy) dataType else dataType.asNullable
        assert(ref.output.head.dataType == expectedType)
        assert(ref.output.head.nullable)
        assert(loop.output.head.dataType == expectedType)
        assert(analyzed.output.head.dataType == expectedType)
      }
    }
  }

  // The legacy setting restores coercion as well as the recursive reference's schema.
  test("SPARK-60860: legacy recursive struct coercion") {
    for {
      legacy <- Seq(false, true)
      rule <- Seq(TypeCoercion.WidenSetOperationTypes, AnsiTypeCoercion.WidenSetOperationTypes)
    } {
      withSQLConf(
        SQLConf.CASE_SENSITIVE.key -> "false",
        SQLConf.LEGACY_RECURSIVE_CTE_USE_ANCHOR_NESTED_NULLABILITY.key -> legacy.toString) {
        val anchorType = StructType(Seq(StructField("x", LongType, nullable = false)))
        val recursionType = StructType(Seq(StructField("X", IntegerType, nullable = true)))
        val anchor = LocalRelation(AttributeReference("s", anchorType)())
        val recursion = LocalRelation(AttributeReference("s", recursionType)())
        val loop = UnionLoop(0, anchor, recursion, anchor.output.map(_.exprId))
        if (legacy) {
          val error = intercept[SparkException] { rule(loop) }
          assert(error.getCondition == "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE")
        } else {
          val widened = rule(loop).asInstanceOf[UnionLoop]
          assert(widened.resolved)
          assert(widened.anchor.output.head.dataType == anchorType)
          assert(widened.recursion.output.head.dataType == anchorType.asNullable)
        }
      }
    }
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE rule on recursive CTE without UnresolvedSubqueryColumnAliases") {
    val cteId = 0
    val anchor = Project(Seq(Alias(Literal(1), "c")()), OneRowRelation())

    def getBeforePlan(): LogicalPlan = {
      val cteRef = CTERelationRef(
        cteId,
        _resolved = false,
        output = Seq(),
        isStreaming = false)
      val recursion = cteRef.copy(recursive = true).subquery("t")
      WithCTE(
        cteRef.copy(recursive = false),
        Seq(CTERelationDef(anchor.union(recursion).subquery("t"), cteId)))
    }

    val analyzed = getAnalyzer.execute(getBeforePlan())

    val outputExprIds = analyzed match {
      case WithCTE(_, cteDefs) =>
        cteDefs.head.child match {
          case SubqueryAlias(_, UnionLoop(_, _, _, exprIds, _, _)) =>
            exprIds
        }
    }

    def getAfterPlan(): LogicalPlan = {
      val recursion = UnionLoopRef(cteId, anchor.output.map(_.withNullability(true)),
        accumulated = false).subquery("t")
      val cteDef = CTERelationDef(UnionLoop(cteId, anchor, recursion,
        outputExprIds).subquery("t"), cteId)
      val cteRef = CTERelationRef(
        cteId,
        _resolved = true,
        output = cteDef.output,
        isStreaming = false)
      WithCTE(cteRef, Seq(cteDef))
    }

    comparePlans(analyzed, getAfterPlan())
  }

  // Motivated by:
  // WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE rule on recursive CTE with UnresolvedSubqueryColumnAliases") {
    val cteId = 0
    val anchor = Project(Seq(Alias(Literal(1), "c")()), OneRowRelation())

    def getBeforePlan(): LogicalPlan = {
      val cteRef = CTERelationRef(
        cteId,
        _resolved = false,
        output = Seq(),
        isStreaming = false)
      val recursion = cteRef.copy(recursive = true).subquery("t")
      val cteDef = CTERelationDef(
        UnresolvedSubqueryColumnAliases(Seq("n"), anchor.union(recursion)).subquery("t"),
        cteId)
      WithCTE(cteRef.copy(recursive = false), Seq(cteDef))
    }

    val analyzed = getAnalyzer.execute(getBeforePlan())

    val outputExprIds = analyzed match {
      case WithCTE(_, cteDefs) =>
        cteDefs.head.child match {
          case SubqueryAlias(_, Project(_, UnionLoop(_, _, _, exprIds, _, _))) =>
            exprIds
        }
    }

    def getAfterPlan(): LogicalPlan = {
      val col = anchor.output.head.withNullability(true)
      val recursion = UnionLoopRef(cteId, anchor.output.map(_.withNullability(true)),
        accumulated = false)
        .select(col.as("n"))
        .subquery("t")
      val cteDef = CTERelationDef(
        UnionLoop(cteId, anchor, recursion, outputExprIds)
          .select(col.as("n")).subquery("t"), cteId)
      val cteRef = CTERelationRef(
        cteId,
        _resolved = true,
        output = cteDef.output,
        isStreaming = false)
      WithCTE(cteRef, Seq(cteDef))
    }

    comparePlans(analyzed, getAfterPlan())
  }
}
