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
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayAppend, ArrayInsert, ArrayPrepend, Attribute, CaseWhen, CreateArray, CreateMap, CreateNamedStruct, ElementAt, GetArrayItem, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructType}

class ResolveRecursiveCTESuite extends AnalysisTest {
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
      // The recursion passes `c` through, so it keeps the anchor's non-nullable type.
      val recursion = UnionLoopRef(cteId, anchor.output, accumulated = false).subquery("t")
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
      val col = anchor.output.head
      val recursion = UnionLoopRef(cteId, anchor.output, accumulated = false)
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

  // Returns the analyzed self-reference and CTE output types and nullability of
  // WITH RECURSIVE t AS (anchor UNION ALL recursion(t)) SELECT * FROM t.
  private def selfReferenceAndOutputTypes(
      anchor: LogicalPlan,
      recursion: LogicalPlan => LogicalPlan)
      : (Seq[(DataType, Boolean)], Seq[(DataType, Boolean)]) = {
    val cteId = 0
    val cteRef = CTERelationRef(cteId, _resolved = false, output = Seq(), isStreaming = false)
    val plan = WithCTE(
      cteRef.copy(recursive = false),
      Seq(CTERelationDef(
        anchor.union(recursion(cteRef.copy(recursive = true).subquery("t"))).subquery("t"),
        cteId)))
    val analyzed = getAnalyzer.execute(plan)
    val refOutput = analyzed.collectFirst { case r: UnionLoopRef => r.output }.get
    def types(attrs: Seq[Attribute]) = attrs.map(a => (a.dataType, a.nullable))
    (types(refOutput), types(analyzed.output))
  }

  private val intArray = ArrayType(IntegerType, containsNull = false)
  private val nullableIntArray = ArrayType(IntegerType, containsNull = true)

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT array(1), map('k', 1), named_struct('f', 1) UNION ALL
  // SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE keeps the anchor's nested types if the recursion passes them through") {
    val anchor = Project(Seq(
      Alias(CreateArray(Seq(Literal(1))), "a")(),
      Alias(CreateMap(Seq(Literal("k"), Literal(1))), "m")(),
      Alias(CreateNamedStruct(Seq(Literal("f"), Literal(1))), "s")()), OneRowRelation())
    val anchorTypes = Seq(
      intArray,
      MapType(StringType, IntegerType, valueContainsNull = false),
      new StructType().add("f", IntegerType, nullable = false)).map(_ -> false)
    assert(anchor.output.map(a => (a.dataType, a.nullable)) === anchorTypes)

    assert(selfReferenceAndOutputTypes(anchor, identity) === (anchorTypes, anchorTypes))
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 AS n, array(1) AS a, array(1) AS b UNION ALL
  // SELECT n + 1, array(n), a FROM t) SELECT * FROM t;
  test("ResolveWithCTE keeps nested types built from self-reference columns that are never NULL") {
    val anchor = Project(Seq(
      Alias(Literal(1), "n")(),
      Alias(CreateArray(Seq(Literal(1))), "a")(),
      Alias(CreateArray(Seq(Literal(1))), "b")()), OneRowRelation())
    val recursion = (t: LogicalPlan) =>
      t.select(($"n" + 1).as("n"), CreateArray(Seq($"n")).as("a"), $"a".as("b"))

    val types = Seq(IntegerType -> false, intArray -> false, intArray -> false)
    assert(selfReferenceAndOutputTypes(anchor, recursion) === (types, types))
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT array(1) AS a, array(1) AS b UNION ALL
  // SELECT array(CAST(NULL AS INT)), a FROM t) SELECT * FROM t;
  test("ResolveWithCTE widens nested types of UnionLoopRef output the recursion writes NULL to") {
    val anchor = Project(Seq(
      Alias(CreateArray(Seq(Literal(1))), "a")(),
      Alias(CreateArray(Seq(Literal(1))), "b")()), OneRowRelation())
    // `b` only becomes nullable after `a` is widened.
    val recursion = (t: LogicalPlan) =>
      t.select(CreateArray(Seq(Literal(null, IntegerType))).as("a"), $"a".as("b"))

    val types = Seq(nullableIntArray -> false, nullableIntArray -> false)
    assert(selfReferenceAndOutputTypes(anchor, recursion) === (types, types))
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 AS n, array(1) AS a UNION ALL
  // SELECT CASE WHEN n > 5 THEN NULL ELSE n + 1 END, array(n) FROM t) SELECT * FROM t;
  test("ResolveWithCTE widens UnionLoopRef output the recursion writes a top-level NULL to") {
    val anchor = Project(Seq(
      Alias(Literal(1), "n")(),
      Alias(CreateArray(Seq(Literal(1))), "a")()), OneRowRelation())
    // `a` only becomes nullable after `n` is widened.
    val recursion = (t: LogicalPlan) => t.select(
      CaseWhen(Seq(($"n" > 5, Literal(null, IntegerType))), $"n" + 1).as("n"),
      CreateArray(Seq($"n")).as("a"))

    val types = Seq(IntegerType -> true, nullableIntArray -> false)
    assert(selfReferenceAndOutputTypes(anchor, recursion) === (types, types))
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT array(1) AS a1, ..., array(1) AS a150 UNION ALL
  // SELECT array(CAST(NULL AS INT)), a1, ..., a149 FROM t) SELECT * FROM t;
  test("ResolveWithCTE widens a long chain of copies within the analyzer's iteration limit") {
    // More links than Resolution iterations, so the loop must not be rebuilt once per link.
    val columns = (1 to 150).map(i => s"a$i")
    val anchor = Project(
      columns.map(c => Alias(CreateArray(Seq(Literal(1))), c)()), OneRowRelation())
    // Each column only becomes nullable after the one it copies is widened.
    val recursion = (t: LogicalPlan) => t.select(
      CreateArray(Seq(Literal(null, IntegerType))).as("a1") +:
        columns.zip(columns.tail).map { case (from, to) => UnresolvedAttribute(from).as(to) }: _*)

    val types = columns.map(_ => nullableIntArray -> false)
    assert(selfReferenceAndOutputTypes(anchor, recursion) === (types, types))
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 AS n, array(1) AS path, array(1) AS prev, array(1) AS head,
  // array(1) AS prev_head UNION ALL SELECT n + 1, array_prepend(array_append(path, n + 1), 0),
  // path, array(path[0]), head FROM t) SELECT * FROM t;
  test("ResolveWithCTE keeps the types of copies of appended arrays and ANSI index reads") {
    val anchor = Project(Seq("n", "path", "prev", "head", "prev_head").map { c =>
      Alias(if (c == "n") Literal(1) else CreateArray(Seq(Literal(1))), c)()
    }, OneRowRelation())
    val recursion = (t: LogicalPlan) => t.select(
      ($"n" + 1).as("n"),
      ArrayPrepend(ArrayAppend($"path", $"n" + 1), Literal(0)).as("path"),
      $"path".as("prev"),
      CreateArray(Seq(GetArrayItem($"path", Literal(0), failOnError = true))).as("head"),
      $"head".as("prev_head"))

    val refTypes = Seq(IntegerType -> false) ++ Seq.fill(4)(intArray -> false)
    val outputTypes = Seq(IntegerType -> false, nullableIntArray -> false, intArray -> false,
      nullableIntArray -> false, intArray -> false)
    assert(selfReferenceAndOutputTypes(anchor, recursion) === (refTypes, outputTypes))
  }

  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 AS n, array(1) AS a, ..., array(1) AS f UNION ALL
  // SELECT n + 1, array_insert(a, 1, n), a, array_insert(c, -1, n), c, array(element_at(e, 1)), e
  // FROM t) SELECT * FROM t;
  test("ResolveWithCTE keeps the types of copies of array_insert at either end and element_at") {
    val anchor = Project(Seq("n", "a", "b", "c", "d", "e", "f").map { c =>
      Alias(if (c == "n") Literal(1) else CreateArray(Seq(Literal(1))), c)()
    }, OneRowRelation())
    val recursion = (t: LogicalPlan) => t.select(
      ($"n" + 1).as("n"),
      ArrayInsert($"a", Literal(1), $"n", legacyNegativeIndex = false).as("a"),
      $"a".as("b"),
      ArrayInsert($"c", Literal(-1), $"n", legacyNegativeIndex = false).as("c"),
      $"c".as("d"),
      CreateArray(Seq(ElementAt($"e", Literal(1), None, failOnError = true))).as("e"),
      $"e".as("f"))

    val refTypes = Seq(IntegerType -> false) ++ Seq.fill(6)(intArray -> false)
    val outputTypes = Seq(IntegerType -> false) ++
      Seq(nullableIntArray, intArray, nullableIntArray, intArray, nullableIntArray, intArray)
        .map(_ -> false)
    assert(selfReferenceAndOutputTypes(anchor, recursion) === (refTypes, outputTypes))
  }

  test("ResolveWithCTE widens copies of appends and index reads that can pad a NULL") {
    val anchor = Project(Seq("n", "path", "prev", "head", "prev_head").map { c =>
      Alias(if (c == "n") Literal(1) else CreateArray(Seq(Literal(1))), c)()
    }, OneRowRelation())
    val recursion = (t: LogicalPlan) => t.select(
      ($"n" + 1).as("n"),
      ArrayAppend($"path", $"n" + 1).as("path"),
      $"path".as("prev"),
      CreateArray(Seq(GetArrayItem($"path", Literal(5), failOnError = false))).as("head"),
      $"head".as("prev_head"))

    withSQLConf(SQLConf.LEGACY_NEGATIVE_INDEX_IN_ARRAY_INSERT.key -> "true") {
      val types = Seq(IntegerType -> false) ++ Seq.fill(4)(nullableIntArray -> false)
      assert(selfReferenceAndOutputTypes(anchor, recursion) === (types, types))
    }
  }

  test("ResolveWithCTE keeps the anchor's nested types with the legacy flag") {
    val anchor = Project(Seq(
      Alias(Literal(1), "n")(),
      Alias(CreateArray(Seq(Literal(1))), "a")()), OneRowRelation())
    val recursion = (t: LogicalPlan) =>
      t.select(($"n" + 1).as("n"), CreateArray(Seq(Literal(null, IntegerType))).as("a"))

    withSQLConf(SQLConf.LEGACY_RECURSIVE_CTE_USE_ANCHOR_NESTED_NULLABILITY.key -> "true") {
      assert(selfReferenceAndOutputTypes(anchor, recursion) ===
        (Seq(IntegerType -> true, intArray -> true),
          Seq(IntegerType -> true, nullableIntArray -> false)))
    }
  }

  test("ResolveWithCTE fails if a self-reference needs wider types but the loop has no origin") {
    val cteId = 0
    val anchor = Project(Seq(Alias(CreateArray(Seq(Literal(1))), "a")()), OneRowRelation())
    val ref = UnionLoopRef(
      cteId, anchor.output.map(_.newInstance().withNullability(true)), accumulated = false)
    // The recursion writes NULL elements, which the self-reference does not allow.
    val recursion = ref.select(CreateArray(Seq(Literal(null, IntegerType))).as("a"))
    val loop = UnionLoop(cteId, anchor, recursion, anchor.output.map(_.newInstance().exprId))
    val cteDef = CTERelationDef(loop.subquery("t"), cteId)
    val plan = WithCTE(
      CTERelationRef(cteId, _resolved = true, output = cteDef.output, isStreaming = false),
      Seq(cteDef))

    val e = intercept[SparkException](ResolveWithCTE(plan))
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains("has no origin to rebuild it from"))
  }
}
