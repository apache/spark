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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite to verify that parameter substitution correctly maps error positions
 * back to the original SQL text. This ensures that error messages point to the
 * correct locations in the user's original query, not the substituted version.
 */
class ParameterPositionMappingSuite extends QueryTest with SharedSparkSession {

  /**
   * Verify that a parameterized SQL query that produces an error
   * has the error context pointing to the original SQL text and correct positions.
   */
  private def checkParameterError[T <: Throwable](
      sql: String,
      expectedStartPos: Option[Int] = None,
      expectedStopPos: Option[Int] = None,
      params: Map[String, Any] = Map.empty,
      positionalParams: Array[Any] = Array.empty)(implicit ct: scala.reflect.ClassTag[T]): T = {

    val exception = intercept[T] {
      if (positionalParams.nonEmpty) {
        spark.sql(sql, positionalParams)
      } else {
        spark.sql(sql, params)
      }
    }

    // For exceptions with query context, verify they reference the original SQL and positions
    exception match {
      case pe: ParseException =>
        if (pe.getQueryContext.nonEmpty) {
          val context = pe.getQueryContext.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(sql)),
            s"Parse error should reference original SQL.\n" +
            s"Expected to contain: $sql\n" +
            s"Actual context SQL: ${context.sqlText}")

          // Verify position mapping if expected positions are provided
          expectedStartPos.foreach { expectedStart =>
            assert(context.originStartIndex.contains(expectedStart),
              s"Start position should be $expectedStart, got: ${context.originStartIndex}")
          }
          expectedStopPos.foreach { expectedStop =>
            assert(context.originStopIndex.contains(expectedStop),
              s"Stop position should be $expectedStop, got: ${context.originStopIndex}")
          }
        }
      case ae: AnalysisException =>
        if (ae.context.nonEmpty) {
          val context = ae.context.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(sql)),
            s"Analysis error should reference original SQL.\n" +
            s"Expected to contain: $sql\n" +
            s"Actual context SQL: ${context.sqlText}")

          // Verify position mapping if expected positions are provided
          expectedStartPos.foreach { expectedStart =>
            assert(context.originStartIndex.contains(expectedStart),
              s"Start position should be $expectedStart, got: ${context.originStartIndex}")
          }
          expectedStopPos.foreach { expectedStop =>
            assert(context.originStopIndex.contains(expectedStop),
              s"Stop position should be $expectedStop, got: ${context.originStopIndex}")
          }
        }
      case _ => // Runtime exceptions may not have query context
    }

    exception
  }

  /**
   * Verify that error positions in parameterized SQL are correctly mapped back to original
   * positions.
   */
  private def checkPositionMapping[T <: Throwable](
      sql: String,
      expectedStartPos: Int,
      expectedStopPos: Int,
      params: Map[String, Any] = Map.empty,
      positionalParams: Array[Any] = Array.empty)(implicit ct: scala.reflect.ClassTag[T]): Unit = {

    val exception = intercept[T] {
      if (positionalParams.nonEmpty) {
        spark.sql(sql, positionalParams)
      } else {
        spark.sql(sql, params)
      }
    }

    // Extract query context and verify position mapping
    val contexts = exception match {
      case pe: ParseException => pe.getQueryContext
      case ae: AnalysisException => ae.context
      case _ => Array.empty[org.apache.spark.QueryContext]
    }

    assert(contexts.nonEmpty, s"Exception should have query context for: $sql")

    val context = contexts.head.asInstanceOf[SQLQueryContext]

    // Verify the SQL text is the original
    assert(context.sqlText.contains(sql),
      s"Context should reference original SQL.\n" +
      s"Expected: $sql\n" +
      s"Actual: ${context.sqlText}")

    // Verify position mapping
    assert(context.originStartIndex.contains(expectedStartPos),
      s"Start position should be $expectedStartPos, got: ${context.originStartIndex}")
    assert(context.originStopIndex.contains(expectedStopPos),
      s"Stop position should be $expectedStopPos, got: ${context.originStopIndex}")
  }

  test("parse error with named parameter") {
    checkParameterError[AnalysisException](
      "SELECT *** :param",
      expectedStartPos = Some(0),
      expectedStopPos = Some(16), // "SELECT *** :param".length - 1 = 17 - 1 = 16
      params = Map("param" -> 42)
    )
  }

  test("parse error with positional parameter") {
    checkParameterError[ParseException](
      "SELECT *** ?",
      expectedStartPos = Some(0),
      expectedStopPos = Some(11), // "SELECT *** ?".length - 1 = 12 - 1 = 11
      positionalParams = Array(42)
    )
  }

  test("analysis error with named parameter") {
    checkParameterError[AnalysisException](
      "SELECT :param FROM nonexistent_table",
      expectedStartPos = Some(0),
      expectedStopPos = Some(35), // length - 1 = 36 - 1 = 35
      params = Map("param" -> 42)
    )
  }

  test("analysis error with positional parameter") {
    checkParameterError[AnalysisException](
      "SELECT ? FROM nonexistent_table",
      expectedStartPos = Some(0),
      expectedStopPos = Some(30), // "SELECT ? FROM nonexistent_table".length - 1 = 31 - 1 = 30
      positionalParams = Array(42)
    )
  }

  test("parse error with identifier clause - SPARK-49757 regression test") {
    val sqlText = "SET CATALOG IDENTIFIER(:param)"
    val exception = checkParameterError[ParseException](
      sqlText,
      params = Map("param" -> "testcat.ns1")
    )

    // Verify specific context details for this test case
    val contexts = exception.getQueryContext
    assert(contexts.nonEmpty, "ParseException should have query context")

    val context = contexts.head.asInstanceOf[SQLQueryContext]
    assert(context.sqlText.contains(sqlText),
      s"Context should contain original SQL: $sqlText")
    assert(context.originStartIndex.contains(0),
      s"Start index should be 0, got: ${context.originStartIndex}")
    assert(context.originStopIndex.contains(sqlText.length - 1),
      s"Stop index should be ${sqlText.length - 1}, got: ${context.originStopIndex}")
  }

  test("multiple parameters with parse error") {
    checkParameterError[ParseException](
      "SELECT :param1, :param2 ***",
      expectedStartPos = Some(0),
      expectedStopPos = Some(27), // "SELECT :param1, :param2 ***".length - 1 = 28 - 1 = 27
      params = Map("param1" -> 10, "param2" -> 20)
    )
  }

  test("parameter substitution with different lengths") {
    checkParameterError[AnalysisException](
      "SELECT :a, :bb, :ccc FROM nonexistent",
      expectedStartPos = Some(0),
      expectedStopPos = Some(37), // length - 1 = 38 - 1 = 37
      params = Map("a" -> 1, "bb" -> 22, "ccc" -> 333)
    )
  }

  test("complex expression with parameter") {
    checkParameterError[ParseException](
      "SELECT CASE WHEN :param > 0 THEN 'yes' *** END",
      expectedStartPos = Some(0),
      expectedStopPos = Some(46), // length - 1 = 47 - 1 = 46
      params = Map("param" -> 5)
    )
  }

  // =============================================================================
  // POSITION MAPPING TESTS
  // =============================================================================

  test("position mapping - error at start of SQL") {
    // Error at position 0: "***" starts at position 0
    checkPositionMapping[ParseException](
      "*** :param",
      expectedStartPos = 0,
      expectedStopPos = 9, // "*** :param".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - error after parameter") {
    // "SELECT :param ***" - error "***" starts at position 14
    checkPositionMapping[ParseException](
      "SELECT :param ***",
      expectedStartPos = 0,
      expectedStopPos = 16, // "SELECT :param ***".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - parameter length difference") {
    // ":x" (2 chars) gets replaced with "42" (2 chars) - no offset change
    checkPositionMapping[ParseException](
      "SELECT :x ***",
      expectedStartPos = 0,
      expectedStopPos = 12, // "SELECT :x ***".length - 1
      params = Map("x" -> 42)
    )
  }

  test("position mapping - long parameter name vs short value") {
    // ":verylongparametername" (22 chars) gets replaced with "1" (1 char)
    checkPositionMapping[ParseException](
      "SELECT :verylongparametername ***",
      expectedStartPos = 0,
      expectedStopPos = 32, // "SELECT :verylongparametername ***".length - 1
      params = Map("verylongparametername" -> 1)
    )
  }

  test("position mapping - short parameter name vs long value") {
    // ":x" (2 chars) gets replaced with "'very long string value'" (25 chars)
    checkPositionMapping[ParseException](
      "SELECT :x ***",
      expectedStartPos = 0,
      expectedStopPos = 12, // "SELECT :x ***".length - 1
      params = Map("x" -> "very long string value")
    )
  }

  test("position mapping - multiple parameters with different lengths") {
    // ":a" -> "1", ":bb" -> "22", ":ccc" -> "333"
    checkPositionMapping[ParseException](
      "SELECT :a, :bb, :ccc ***",
      expectedStartPos = 0,
      expectedStopPos = 23, // "SELECT :a, :bb, :ccc ***".length - 1
      params = Map("a" -> 1, "bb" -> 22, "ccc" -> 333)
    )
  }

  test("position mapping - positional parameters") {
    // "SELECT ? ***" - error at end
    checkPositionMapping[ParseException](
      "SELECT ? ***",
      expectedStartPos = 0,
      expectedStopPos = 11, // "SELECT ? ***".length - 1
      positionalParams = Array(42)
    )
  }

  test("position mapping - multiple positional parameters") {
    // "SELECT ?, ?, ? ***"
    checkPositionMapping[ParseException](
      "SELECT ?, ?, ? ***",
      expectedStartPos = 0,
      expectedStopPos = 17, // "SELECT ?, ?, ? ***".length - 1
      positionalParams = Array(1, 22, 333)
    )
  }

  test("position mapping - analysis error with specific position") {
    // Analysis error should also preserve positions
    checkPositionMapping[AnalysisException](
      "SELECT :param FROM nonexistent_table",
      expectedStartPos = 0,
      expectedStopPos = 35, // "SELECT :param FROM nonexistent_table".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - nested expression with parameter") {
    // Complex nested expression
    checkPositionMapping[ParseException](
      "SELECT (:param + 10) * *** FROM test",
      expectedStartPos = 0,
      expectedStopPos = 35, // "SELECT (:param + 10) * *** FROM test".length - 1
      params = Map("param" -> 5)
    )
  }

  test("position mapping - parameter in WHERE clause") {
    // Parameter in WHERE clause with error
    checkPositionMapping[AnalysisException](
      "SELECT * FROM test WHERE id = :param AND undefined_column = 1",
      expectedStartPos = 0,
      expectedStopPos = 61, // full SQL length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - string parameter with quotes") {
    // String parameter that affects quoting
    checkPositionMapping[ParseException](
      "SELECT :param ***",
      expectedStartPos = 0,
      expectedStopPos = 16, // "SELECT :param ***".length - 1
      params = Map("param" -> "string with 'quotes'")
    )
  }

  test("position mapping - null parameter") {
    // Null parameter substitution
    checkPositionMapping[ParseException](
      "SELECT :param ***",
      expectedStartPos = 0,
      expectedStopPos = 16, // "SELECT :param ***".length - 1
      params = Map("param" -> null)
    )
  }
}
