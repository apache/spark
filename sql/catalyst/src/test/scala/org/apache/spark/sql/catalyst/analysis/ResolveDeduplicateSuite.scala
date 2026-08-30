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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, DeduplicateAllColumnsAsKey, DeduplicateKeyColumns, DeduplicateWithinWatermark, LocalRelation, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Unit tests for [[ResolveDeduplicate]], the analyzer rule that resolves [[UnresolvedDeduplicate]]
 * into a [[Deduplicate]] / [[DeduplicateWithinWatermark]]. The rule is applied directly so the
 * deterministic-key-order conf can be toggled per case. See SPARK-57489.
 */
class ResolveDeduplicateSuite extends AnalysisTest {

  private val a = $"a".int
  private val b = $"b".int
  private val c = $"c".int
  private val rel = LocalRelation(c, a, b)

  private val confKey = SQLConf.DROP_DUPLICATES_DETERMINISTIC_KEY_ORDER.key

  private def resolveKeys(
      columnNames: Seq[String],
      allColumnsAsKeys: Boolean = false,
      withinWatermark: Boolean = false,
      viaSparkClassic: Boolean = true,
      child: LogicalPlan = rel): Seq[Attribute] = {
    val keySpec =
      if (allColumnsAsKeys) DeduplicateAllColumnsAsKey else DeduplicateKeyColumns(columnNames)
    ResolveDeduplicate(UnresolvedDeduplicate(
      keySpec, withinWatermark, viaSparkClassic, child)) match {
      case d: Deduplicate => d.keys
      case d: DeduplicateWithinWatermark => d.keys
      case other => fail(s"Expected a (Deduplicate|DeduplicateWithinWatermark), got: $other")
    }
  }

  test("SPARK-57489: deterministic resolution keeps first-occurrence input order and dedups") {
    withSQLConf(confKey -> "true") {
      // The input order is preserved and duplicate names are collapsed at first occurrence. Under
      // the old `toSet.toSeq` this order would not be guaranteed; asserting it exactly guards
      // against anyone reintroducing Set/distinct-based ordering.
      assert(resolveKeys(Seq("c", "a", "c", "b")).map(_.name) === Seq("c", "a", "b"))
      assert(resolveKeys(Seq("b", "b", "a")).map(_.name) === Seq("b", "a"))
    }
  }

  test("SPARK-57489: deterministic resolution identical across engines (interop)") {
    // The deterministic path ignores the engine flag, so Classic and Connect resolve the SAME
    // attributes (same exprIds, same order). This is what lets a checkpoint written by one engine
    // restart under the other.
    withSQLConf(confKey -> "true") {
      val classicSubset = resolveKeys(Seq("c", "a", "c", "b"), viaSparkClassic = true)
      val connectSubset = resolveKeys(Seq("c", "a", "c", "b"), viaSparkClassic = false)
      assert(classicSubset.map(_.exprId) === connectSubset.map(_.exprId))
      assert(classicSubset.map(_.name) === Seq("c", "a", "b"))

      val classicAll = resolveKeys(Nil, allColumnsAsKeys = true, viaSparkClassic = true)
      val connectAll = resolveKeys(Nil, allColumnsAsKeys = true, viaSparkClassic = false)
      assert(classicAll.map(_.exprId) === connectAll.map(_.exprId))
      assert(classicAll === rel.output)
    }
  }

  test("SPARK-57489: legacy fallback - Classic dedups the subset (Set order)") {
    withSQLConf(confKey -> "false") {
      val keys = resolveKeys(Seq("c", "a", "c", "b"), viaSparkClassic = true)
      // Classic legacy used `toSet.toSeq`: duplicate names are collapsed (order is Set-defined and
      // intentionally not asserted here).
      assert(keys.length === 3)
      assert(keys.map(_.name).toSet === Set("a", "b", "c"))
    }
  }

  test("SPARK-57489: legacy fallback - Connect keeps duplicates in input order") {
    withSQLConf(confKey -> "false") {
      val keys = resolveKeys(Seq("c", "a", "c", "b"), viaSparkClassic = false)
      // Connect legacy did not dedup the requested names.
      assert(keys.map(_.name) === Seq("c", "a", "c", "b"))
    }
  }

  test("SPARK-57489: allColumnsAsKeys - deterministic and Connect legacy use child output order") {
    // Deterministic (regardless of engine flag) keys on all child columns in output order.
    withSQLConf(confKey -> "true") {
      assert(
        resolveKeys(Nil, allColumnsAsKeys = true, viaSparkClassic = true) === rel.output)
      assert(
        resolveKeys(Nil, allColumnsAsKeys = true, viaSparkClassic = false) === rel.output)
    }
    // Legacy Spark Connect also keys on all child columns in output order.
    withSQLConf(confKey -> "false") {
      assert(
        resolveKeys(Nil, allColumnsAsKeys = true, viaSparkClassic = false) === rel.output)
    }
  }

  test("SPARK-57489: allColumnsAsKeys - Classic legacy fallback uses Set order over all columns") {
    withSQLConf(confKey -> "false") {
      // Legacy Classic `dropDuplicates()` resolved `columns.toSet.toSeq`: every column is present
      // (order is Set-defined and intentionally not asserted here).
      val keys = resolveKeys(Nil, allColumnsAsKeys = true, viaSparkClassic = true)
      assert(keys.map(_.exprId).toSet === rel.output.map(_.exprId).toSet)
    }
  }

  test("SPARK-57489: withinWatermark resolves to DeduplicateWithinWatermark") {
    val resolved = ResolveDeduplicate(
      UnresolvedDeduplicate(DeduplicateKeyColumns(Seq("a")), withinWatermark = true,
        viaSparkClassic = true, rel))
    assert(resolved.isInstanceOf[DeduplicateWithinWatermark])
    assert(resolved.asInstanceOf[DeduplicateWithinWatermark].keys.map(_.name) === Seq("a"))
  }

  test("SPARK-57489: duplicate-named columns produce multiple keys (filter, not find)") {
    val a2 = $"a".int
    val dupRel = LocalRelation(a, a2)
    // A single requested name "a" must resolve to BOTH same-named columns (filter, not find) in
    // every mode. Compare by exprId so the test fails if we returned the same attribute twice
    // instead of the two distinct same-named columns.
    Seq(
      ("true", true), // deterministic (engine flag ignored)
      ("true", false),
      ("false", true), // legacy Spark Classic
      ("false", false) // legacy Spark Connect
    ).foreach { case (deterministic, legacy) =>
      withSQLConf(confKey -> deterministic) {
        assert(
          resolveKeys(Seq("a"), viaSparkClassic = legacy, child = dupRel).map(_.exprId)
            === Seq(a.exprId, a2.exprId),
          s"deterministic=$deterministic, viaSparkClassic=$legacy")
      }
    }
  }

  test("SPARK-57489: unknown column name fails analysis") {
    checkError(
      exception = intercept[AnalysisException] {
        ResolveDeduplicate(UnresolvedDeduplicate(
          DeduplicateKeyColumns(Seq("does_not_exist")), withinWatermark = false,
          viaSparkClassic = true, rel))
      },
      condition = "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES",
      parameters = Map("colName" -> "does_not_exist", "fieldNames" -> "c, a, b"))
  }
}
