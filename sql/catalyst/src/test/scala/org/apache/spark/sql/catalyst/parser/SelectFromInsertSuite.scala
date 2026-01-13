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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Parser test cases for NEW TABLE(INSERT...) syntax.
 *
 * This feature allows selecting rows that were just inserted by an INSERT statement.
 * Syntax: SELECT * FROM NEW TABLE(INSERT INTO table ...) [AS alias]
 *
 * Restrictions (grammar-enforced):
 * 1. Top-level SELECT only (no subqueries or CTEs)
 * 2. Single table reference (no joins)
 * 3. No PIVOT/UNPIVOT operations
 */
class SelectFromInsertSuite extends AnalysisTest {
  import CatalystSqlParser._

  private def parseAndValidate(sqlCommand: String): LogicalPlan = {
    parsePlan(sqlCommand)
  }

  private def parseException(sqlText: String): SparkThrowable = {
    super.parseException(parsePlan)(sqlText)
  }

  // ============================================================================
  // POSITIVE TEST CASES - Should parse successfully
  // ============================================================================

  test("NEW TABLE: basic INSERT INTO with VALUES") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO t VALUES (1, 2))")
    assert(plan != null)
  }

  test("NEW TABLE: with table alias") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO t VALUES (1, 2)) AS inserted")
    assert(plan != null)
  }

  test("NEW TABLE: with table alias and column list") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO t VALUES (1, 2)) AS inserted(col1, col2)")
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with SELECT") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO target SELECT * FROM source)")
    assert(plan != null)
  }

  test("NEW TABLE: INSERT OVERWRITE") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT OVERWRITE TABLE t SELECT * FROM source) AS t")
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with partition") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO t PARTITION(year=2024) VALUES (1, 2)) AS new_rows")
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with column list") {
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO t(id, name) VALUES (1, 'Alice')) AS inserted")
    assert(plan != null)
  }

  test("NEW TABLE: with WHERE clause") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2))
         |WHERE col1 > 0""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with GROUP BY") {
    val plan = parseAndValidate(
      """SELECT dept, COUNT(*)
         |FROM NEW TABLE(
         |  INSERT INTO employees VALUES
         |    (1, 'Alice', 'Eng'),
         |    (2, 'Bob', 'Eng')
         |) AS e(id, name, dept)
         |GROUP BY dept""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with HAVING") {
    val plan = parseAndValidate(
      """SELECT dept, COUNT(*) as cnt
         |FROM NEW TABLE(INSERT INTO employees VALUES (1, 'Alice', 'Eng'))
         |GROUP BY dept
         |HAVING COUNT(*) > 0""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: multiple columns in VALUES") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO employees(id, name, dept, salary)
         |  VALUES
         |    (1, 'Alice', 'Engineering', 100000),
         |    (2, 'Bob', 'Sales', 80000),
         |    (3, 'Charlie', 'Marketing', 90000)
         |) AS new_employees""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: INSERT INTO ... SELECT with WHERE") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO archive
         |  SELECT * FROM current_data WHERE created_date < '2024-01-01'
         |) AS archived_rows
         |WHERE archived_rows.status = 'active'""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: INSERT OVERWRITE with partition spec") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT OVERWRITE TABLE partitioned_table
         |  PARTITION(year=2024, month=1)
         |  SELECT id, name, value FROM source
         |) AS overwritten""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with WITH clause (table options)") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO TABLE external_table
         |  WITH (path '/tmp/data')
         |  VALUES (1, 2, 3)
         |) AS inserted""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with BY NAME") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO target BY NAME
         |  SELECT * FROM source
         |) AS inserted_by_name""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with ORDER BY and LIMIT") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 'a'), (2, 'b'))
         |ORDER BY col1
         |LIMIT 10""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with WINDOW clause") {
    val plan = parseAndValidate(
      """SELECT id, name,
         |       ROW_NUMBER() OVER w as rn
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'))
         |WINDOW w AS (ORDER BY id)""".stripMargin)
    assert(plan != null)
  }

  // INSERT OVERWRITE DIRECTORY is explicitly not supported by AstBuilder
  // This test verifies the error message
  test("NEW TABLE: INSERT OVERWRITE DIRECTORY not supported") {
    val e = intercept[SparkException] {
      parseAndValidate(
        """SELECT *
           |FROM NEW TABLE(
           |  INSERT OVERWRITE DIRECTORY '/tmp/output'
           |  SELECT * FROM source
           |)""".stripMargin)
    }
    // Verify it's the expected "not supported" error
    assert(e.getMessage.contains("not supported"))
  }

  test("NEW TABLE: complex WHERE with multiple conditions") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2, 3))
         |WHERE col1 > 0 AND col2 < 10 OR col3 = 3""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with aggregate functions") {
    val plan = parseAndValidate(
      """SELECT COUNT(*), SUM(salary), AVG(salary)
         |FROM NEW TABLE(
         |  INSERT INTO employees VALUES (1, 'Alice', 100000)
         |)""".stripMargin)
    assert(plan != null)
  }

  // ============================================================================
  // NEGATIVE TEST CASES - Should fail at parse time
  // ============================================================================
  // Note: Tests for context restrictions (subquery, CTE, UNION, nesting) are in
  // SelectFromInsertAnalysisSuite.scala as they require analysis-level checking

  test("NEW TABLE: cannot use with JOIN") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2)) n
         |JOIN other_table o ON n.id = o.id""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: cannot use with comma-separated tables") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2)), other_table""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: cannot use with PIVOT") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 'a', 100))
         |PIVOT (SUM(amount) FOR category IN ('a', 'b'))""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: cannot use with UNPIVOT") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 100, 200))
         |UNPIVOT (value FOR column IN (col1, col2))""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: cannot use with LATERAL VIEW") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, ARRAY(1,2,3)))
         |LATERAL VIEW explode(arr) t AS val""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: missing INSERT keyword") {
    val e = parseException(
      "SELECT * FROM NEW TABLE(SELECT * FROM t)")
    // Exception thrown as expected
  }

  test("NEW TABLE: missing TABLE keyword") {
    val e = parseException(
      "SELECT * FROM NEW (INSERT INTO t VALUES (1, 2))")
    // Exception thrown as expected
  }

  test("NEW TABLE: missing parentheses") {
    val e = parseException(
      "SELECT * FROM NEW TABLE INSERT INTO t VALUES (1, 2)")
    // Exception thrown as expected
  }

  test("NEW TABLE: incomplete INSERT statement") {
    val e = parseException(
      "SELECT * FROM NEW TABLE(INSERT INTO)")
    // Exception thrown as expected
  }

  test("NEW TABLE: cannot use with CROSS JOIN") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2))
         |CROSS JOIN other_table""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: cannot use with LEFT JOIN") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2)) n
         |LEFT JOIN other_table o ON n.id = o.id""".stripMargin)
    // Exception thrown as expected
  }

  // TODO: Add tests for FROM subquery and nested NEW TABLE with analysis-level checks

  // ============================================================================
  // EDGE CASES
  // ============================================================================

  test("NEW TABLE: empty INSERT (syntactically valid, may fail in analysis)") {
    // This may parse but should fail during analysis
    val plan = parseAndValidate(
      "SELECT * FROM NEW TABLE(INSERT INTO t SELECT * FROM empty_table)")
    assert(plan != null)
  }

  test("NEW TABLE: with complex expressions in SELECT") {
    val plan = parseAndValidate(
      """SELECT col1 + col2 as sum,
         |       CASE WHEN col1 > 0 THEN 'positive' ELSE 'negative' END as sign
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2))""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with subquery in INSERT SELECT") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO target
         |  SELECT * FROM (SELECT id, name FROM source WHERE active = true)
         |)""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with UNION in INSERT SELECT") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO target
         |  SELECT * FROM source1 UNION SELECT * FROM source2
         |)""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with IF NOT EXISTS is rejected") {
    // IF NOT EXISTS is only supported with INSERT OVERWRITE TABLE, not INSERT INTO
    val e = intercept[ParseException] {
      parseAndValidate(
        """SELECT *
           |FROM NEW TABLE(
           |  INSERT INTO t PARTITION(year=2024) IF NOT EXISTS
           |  VALUES (1, 2)
           |)""".stripMargin)
    }
    assert(e.getMessage.contains("IF NOT EXISTS"))
  }

  test("NEW TABLE: with DISTINCT in outer SELECT") {
    val plan = parseAndValidate(
      """SELECT DISTINCT col1
         |FROM NEW TABLE(INSERT INTO t VALUES (1), (1), (2))""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with column aliases and no AS keyword") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2)) result(a, b)""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with backtick-quoted identifiers") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO `my-table` VALUES (1, 2)) AS `my-alias`""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with string literals containing special characters") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO t VALUES (1, 'O''Brien', 'Line1\nLine2')
         |)""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with multiple partitions") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO t PARTITION(year=2024, month=1, day=15)
         |  VALUES (1, 2, 3)
         |)""".stripMargin)
    assert(plan != null)
  }

  // ============================================================================
  // COMMENT TESTS
  // ============================================================================

  test("NEW TABLE: with single-line comments") {
    val plan = parseAndValidate(
      """-- This is a comment
         |SELECT * -- selecting all columns
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2)) -- inserted data
         |WHERE col1 > 0 -- filter condition""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with block comments") {
    val plan = parseAndValidate(
      """/* Multi-line
         |   comment */
         |SELECT *
         |FROM NEW TABLE(/* inline comment */ INSERT INTO t VALUES (1, 2))""".stripMargin)
    assert(plan != null)
  }

  // ============================================================================
  // KEYWORD TESTS - NEW should be non-reserved
  // ============================================================================

  test("NEW as column name in regular query") {
    val plan = parseAndValidate(
      "SELECT new FROM t")
    assert(plan != null)
  }

  test("NEW as table alias in regular query") {
    val plan = parseAndValidate(
      "SELECT * FROM t AS new")
    assert(plan != null)
  }

  test("NEW as column alias") {
    val plan = parseAndValidate(
      "SELECT col1 AS new FROM t")
    assert(plan != null)
  }

  test("NEW as backtick-quoted identifier") {
    val plan = parseAndValidate(
      "SELECT * FROM `NEW`")
    assert(plan != null)
  }

  // ============================================================================
  // INTEGRATION WITH OTHER SQL FEATURES
  // ============================================================================

  test("NEW TABLE: with hints") {
    val plan = parseAndValidate(
      """SELECT /*+ BROADCAST(t) */ *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2))""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: with TABLESAMPLE (should fail - not in grammar)") {
    val e = parseException(
      """SELECT *
         |FROM NEW TABLE(INSERT INTO t VALUES (1, 2))
         |TABLESAMPLE (10 PERCENT)""".stripMargin)
    // Exception thrown as expected
  }

  test("NEW TABLE: with common table expressions in INSERT") {
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT INTO target
         |  WITH cte AS (SELECT * FROM source)
         |  SELECT * FROM cte
         |)""".stripMargin)
    assert(plan != null)
  }

  test("NEW TABLE: INSERT with OVERWRITE and IF NOT EXISTS (mutually exclusive)") {
    // This should parse but may fail during semantic analysis
    val plan = parseAndValidate(
      """SELECT *
         |FROM NEW TABLE(
         |  INSERT OVERWRITE TABLE t PARTITION(year=2024) IF NOT EXISTS
         |  VALUES (1, 2)
         |)""".stripMargin)
    assert(plan != null)
  }
}
