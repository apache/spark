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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, UnresolvedGapFill}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DayTimeIntervalType

class ResolveGapFillSuite extends AnalysisTest {

  private def unresolved(child: LogicalPlan): UnresolvedGapFill =
    UnresolvedGapFill(
      partitionColumns = Nil,
      timeColumn = $"ts",
      binWidthExpr = Literal(5L * 60L * 1000000L, DayTimeIntervalType()),
      originExpr = None,
      rangeStartExpr = None,
      rangeEndExpr = None,
      fillColumns = Nil,
      maxGapExpr = None,
      binStart = None,
      isGenerated = None,
      child = child)

  private val resolvedInput = LocalRelation($"ts".timestampNTZ)
  private val missingTable = UnresolvedRelation(Seq("missing"))

  private def expectNotSupported(plan: LogicalPlan): Unit =
    checkError(
      exception = intercept[AnalysisException](ResolveGapFill.apply(plan)),
      condition = "UNSUPPORTED_FEATURE.GAP_FILL",
      sqlState = "0A000",
      parameters = Map.empty[String, String])

  test("GAP FILL is rejected as not yet supported once its child is resolved") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      val waiting = unresolved(missingTable)
      // A missing table must still win, so the node is left alone until its child resolves.
      assert(ResolveGapFill.apply(waiting) == waiting)
      expectNotSupported(unresolved(resolvedInput))
    }
  }

  test("GAP FILL is rejected at once when the operator is disabled") {
    // The gate does not wait for the child, so a missing table also gets this error.
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "false") {
      expectNotSupported(unresolved(missingTable))
      expectNotSupported(unresolved(resolvedInput))
    }
  }

  test("GAP FILL errors through the full analyzer") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      assertAnalysisErrorCondition(
        unresolved(resolvedInput), "UNSUPPORTED_FEATURE.GAP_FILL", Map.empty)
      assertAnalysisErrorCondition(
        unresolved(missingTable), "TABLE_OR_VIEW_NOT_FOUND", Map("relationName" -> "`missing`"))
    }
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "false") {
      assertAnalysisErrorCondition(
        unresolved(missingTable), "UNSUPPORTED_FEATURE.GAP_FILL", Map.empty)
    }
  }
}
