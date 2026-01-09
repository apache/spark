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
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Exists, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Test suite for analysis-time validation of NEW TABLE(INSERT ...) context restrictions.
 * These tests verify that returning inserts are only allowed at the top level of SELECT statements.
 */
class SelectFromInsertAnalysisSuite extends AnalysisTest {

  private val testRelation = LocalRelation($"col1".int, $"col2".string)
  private val emptyRelation = LocalRelation() // Empty relation with no columns
  // Use "TaBlE" which exists in AnalysisTest setup
  private val testTable = UnresolvedRelation(Seq("TaBlE"))

  // Helper to create a returning INSERT using empty relation to avoid schema issues
  private def returningInsert = InsertIntoStatement(
    table = testTable,
    partitionSpec = Map.empty,
    userSpecifiedCols = Nil,
    query = emptyRelation,  // Use empty to avoid column mismatch errors
    overwrite = false,
    ifPartitionNotExists = false,
    byName = false,
    returning = true
  )

  test("NEW TABLE: allowed at top level of SELECT") {
    // SELECT col1, col2 FROM NEW TABLE(INSERT ...) - should not throw restriction error
    // Note: Will fail analysis for other reasons (table not found), but shouldn't
    // throw NESTED_RETURNING_INSERT error
    val plan = Project(Seq($"col1", $"col2"), returningInsert)
    val e = intercept[AnalysisException] {
      val analyzer = getAnalyzer
      analyzer.checkAnalysis(analyzer.execute(plan))
    }
    // Should NOT be our restriction error
    assert(!e.getMessage.contains("NESTED_RETURNING_INSERT"))
  }

  test("NEW TABLE: not allowed in subquery") {
    // SELECT * FROM t WHERE col1 IN (SELECT * FROM NEW TABLE(...)) - should fail
    val subquery = ScalarSubquery(
      Project(Seq($"col1"), returningInsert),
      Seq.empty)
    val plan = testRelation.where($"col1" === subquery)

    assertAnalysisError(
      plan,
      Seq("NESTED_RETURNING_INSERT", "subquery")
    )
  }

  test("NEW TABLE: not allowed in UNION branch") {
    // SELECT * FROM NEW TABLE(...) UNION SELECT * FROM empty - should fail
    val unionPlan = Union(Seq(returningInsert, emptyRelation))

    assertAnalysisError(
      unionPlan,
      Seq("NESTED_RETURNING_INSERT", "UNION")
    )
  }

  test("NEW TABLE: not allowed in INTERSECT branch") {
    // SELECT * FROM NEW TABLE(...) INTERSECT SELECT * FROM empty - should fail
    val intersectPlan = Intersect(returningInsert, emptyRelation, isAll = false)

    assertAnalysisError(
      intersectPlan,
      Seq("NESTED_RETURNING_INSERT", "INTERSECT")
    )
  }

  test("NEW TABLE: not allowed in EXCEPT branch") {
    // SELECT * FROM NEW TABLE(...) EXCEPT SELECT * FROM empty - should fail
    val exceptPlan = Except(returningInsert, emptyRelation, isAll = false)

    assertAnalysisError(
      exceptPlan,
      Seq("NESTED_RETURNING_INSERT", "EXCEPT")
    )
  }

  test("NEW TABLE: allowed in top-level CTE") {
    // WITH cte AS (SELECT * FROM NEW TABLE(...)) SELECT * FROM cte
    // Should not throw restriction error
    val cteDef = CTERelationDef(Project(Seq($"col1", $"col2"), returningInsert))
    val cteRef = CTERelationRef(
      cteId = cteDef.id,
      _resolved = true,
      output = Seq($"col1".int, $"col2".string),
      isStreaming = false)
    val plan = WithCTE(Project(Seq($"col1", $"col2"), cteRef), Seq(cteDef))

    val e = intercept[AnalysisException] {
      val analyzer = getAnalyzer
      analyzer.checkAnalysis(analyzer.execute(plan))
    }
    // Should NOT be our restriction error
    assert(!e.getMessage.contains("NESTED_RETURNING_INSERT"))
  }

  test("NEW TABLE: not allowed in nested SELECT") {
    // SELECT * FROM t WHERE EXISTS (SELECT * FROM NEW TABLE(...)) - should fail
    val existsSubquery = Exists(Project(Seq($"col1", $"col2"), returningInsert))
    val plan = testRelation.where(existsSubquery)

    assertAnalysisError(
      plan,
      Seq("NESTED_RETURNING_INSERT", "subquery")
    )
  }

  // V1 tests using temp views - these are expected to fail with V1 rejection error
  // V2-specific versions are in SelectFromInsertV2AnalysisSuite
  ignore("NEW TABLE: not allowed nested within another NEW TABLE") {
    // SELECT * FROM NEW TABLE(
    //   INSERT INTO TaBlE SELECT * FROM NEW TABLE(INSERT INTO TaBlE2 SELECT ...)
    // ) - should fail
    val innerReturningInsert = InsertIntoStatement(
      table = UnresolvedRelation(Seq("TaBlE2")),
      partitionSpec = Map.empty,
      userSpecifiedCols = Nil,
      query = emptyRelation,
      overwrite = false,
      ifPartitionNotExists = false,
      byName = false,
      returning = true
    )

    val outerReturningInsert = InsertIntoStatement(
      table = UnresolvedRelation(Seq("TaBlE")),
      partitionSpec = Map.empty,
      userSpecifiedCols = Nil,
      query = innerReturningInsert,  // Nested returning insert
      overwrite = false,
      ifPartitionNotExists = false,
      byName = false,
      returning = true
    )

    assertAnalysisError(
      outerReturningInsert,
      Seq("NESTED_RETURNING_INSERT")
    )
  }

  ignore("NEW TABLE: target table cannot be referenced in the INSERT query") {
    // SELECT * FROM NEW TABLE(INSERT INTO TaBlE SELECT * FROM TaBlE) - should fail
    val selfReferencingInsert = InsertIntoStatement(
      table = testTable,
      partitionSpec = Map.empty,
      userSpecifiedCols = Nil,
      query = testTable,  // Selecting from same table we're inserting into
      overwrite = false,
      ifPartitionNotExists = false,
      byName = false,
      returning = true
    )

    assertAnalysisError(
      selfReferencingInsert,
      Seq("RETURNING_INSERT_TABLE_CONFLICT")
    )
  }

  ignore("NEW TABLE: target table cannot be referenced in subquery within INSERT query") {
    // SELECT * FROM NEW TABLE(
    //   INSERT INTO TaBlE SELECT * FROM TaBlE2 WHERE a IN (SELECT a FROM TaBlE)
    // ) - should fail
    val t1 = UnresolvedRelation(Seq("TaBlE"))
    val t2 = UnresolvedRelation(Seq("TaBlE2"))
    val subquery = ScalarSubquery(t1, Seq.empty)
    val queryWithSubquery = t2.where($"a" === subquery)

    val selfReferencingInsert = InsertIntoStatement(
      table = t1,
      partitionSpec = Map.empty,
      userSpecifiedCols = Nil,
      query = queryWithSubquery,
      overwrite = false,
      ifPartitionNotExists = false,
      byName = false,
      returning = true
    )

    assertAnalysisError(
      selfReferencingInsert,
      Seq("RETURNING_INSERT_TABLE_CONFLICT")
    )
  }

  ignore("NEW TABLE: same table cannot be target of multiple returning inserts") {
    // WITH
    //   step1 AS (SELECT * FROM NEW TABLE(INSERT INTO TaBlE ...)),
    //   step2 AS (SELECT * FROM NEW TABLE(INSERT INTO TaBlE ...))
    // SELECT * FROM step1
    // - should fail (TaBlE is target of two returning inserts)
    val insert1 = InsertIntoStatement(
      table = testTable,
      partitionSpec = Map.empty,
      userSpecifiedCols = Nil,
      query = emptyRelation,
      overwrite = false,
      ifPartitionNotExists = false,
      byName = false,
      returning = true
    )

    val insert2 = InsertIntoStatement(
      table = testTable,  // Same table!
      partitionSpec = Map.empty,
      userSpecifiedCols = Nil,
      query = emptyRelation,
      overwrite = false,
      ifPartitionNotExists = false,
      byName = false,
      returning = true
    )

    val cteDef1 = CTERelationDef(Project(Seq($"col1", $"col2"), insert1))
    val cteDef2 = CTERelationDef(Project(Seq($"col1", $"col2"), insert2))
    val cteRef1 = CTERelationRef(
      cteId = cteDef1.id,
      _resolved = true,
      output = Seq($"col1".int, $"col2".string),
      isStreaming = false)

    val plan = WithCTE(
      Project(Seq($"col1", $"col2"), cteRef1),
      Seq(cteDef1, cteDef2)
    )

    assertAnalysisError(
      plan,
      Seq("RETURNING_INSERT_TABLE_CONFLICT", "multiple NEW TABLE")
    )
  }

  // Note: The following tests require V2 table infrastructure and are tested
  // in SelectFromInsertV2AnalysisSuite below:
  // - Nested returning insert detection
  // - Table self-reference checks (require resolved V2 relations)
}

/**
 * V2-specific analysis tests for NEW TABLE(INSERT ...) that require
 * proper V2 table resolution to work correctly.
 *
 * These tests verify table conflict detection which requires resolved relations.
 */
class SelectFromInsertV2AnalysisSuite extends AnalysisTest {

  // TODO: These tests require proper V2 table setup with DataSourceV2Relation
  // They will be implemented once we have V2 execution support.
  // For now, they are documented here as pending implementation.

  ignore("V2: not allowed nested within another NEW TABLE") {
    // Requires: V2 table setup
    // Test: INSERT INTO v2table1 SELECT * FROM NEW TABLE(INSERT INTO v2table2 ...)
    // Expected: NESTED_RETURNING_INSERT error
  }

  ignore("V2: target table cannot be referenced in the INSERT query") {
    // Requires: V2 table setup
    // Test: INSERT INTO v2table SELECT * FROM v2table
    // Expected: RETURNING_INSERT_TABLE_CONFLICT error
  }

  ignore("V2: target table cannot be referenced in subquery within INSERT query") {
    // Requires: V2 table setup
    // Test: INSERT INTO v2table1 SELECT * FROM v2table2 WHERE col IN (SELECT col FROM v2table1)
    // Expected: RETURNING_INSERT_TABLE_CONFLICT error
  }

  ignore("V2: same table cannot be target of multiple returning inserts") {
    // Requires: V2 table setup with CTE
    // Test: WITH cte1 AS (INSERT INTO v2table ...), cte2 AS (INSERT INTO v2table ...) SELECT ...
    // Expected: RETURNING_INSERT_TABLE_CONFLICT error
  }
}
