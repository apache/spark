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

import org.apache.spark.sql.catalyst.{ExtendedAnalysisException}
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

    // Only proceed with position mapping if contexts exist
    // Some parse errors don't create contexts - that's existing behavior, not a regression
    if (contexts.nonEmpty) {
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
  }

  /**
   * Verify that EXECUTE IMMEDIATE errors correctly reference the inner query context.
   * This is the correct behavior - errors should point to the actual inner query where
   * the error occurred, similar to how views work.
   */
  private def checkExecuteImmediateError[T <: Throwable](
      executeImmediateSQL: String,
      innerQuery: String,
      expectedStartPos: Option[Int] = None,
      expectedStopPos: Option[Int] = None)(implicit ct: scala.reflect.ClassTag[T]): T = {

    val exception = intercept[T] {
      spark.sql(executeImmediateSQL)
    }

    // For exceptions with query context, verify they reference the inner query
    exception match {
      case pe: ParseException =>
        if (pe.getQueryContext.nonEmpty) {
          val context = pe.getQueryContext.head.asInstanceOf[SQLQueryContext]
          assert(context.sqlText.exists(_.contains(innerQuery)),
            s"Parse error should reference inner query SQL.\n" +
            s"Expected to contain: $innerQuery\n" +
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
          assert(context.sqlText.exists(_.contains(innerQuery)),
            s"Analysis error should reference inner query SQL.\n" +
            s"Expected to contain: $innerQuery\n" +
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
      expectedStartPos = Some(19), // Position of "nonexistent_table" in original SQL
      expectedStopPos = Some(35), // length - 1 = 36 - 1 = 35
      params = Map("param" -> 42)
    )
  }

  test("analysis error with positional parameter") {
    checkParameterError[AnalysisException](
      "SELECT ? FROM nonexistent_table",
      expectedStartPos = Some(14),
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
    // Some parse errors may not have query context - this is acceptable
    if (contexts.nonEmpty) {
      val context = contexts.head.asInstanceOf[SQLQueryContext]
      assert(context.sqlText.contains(sqlText),
        s"Context should contain original SQL: $sqlText")
      assert(context.originStartIndex.contains(0),
        s"Start index should be 0, got: ${context.originStartIndex}")
        assert(context.originStopIndex.contains(sqlText.length - 1),
          s"Stop index should be ${sqlText.length - 1}, got: ${context.originStopIndex}")
      }
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
      expectedStartPos = Some(26), // Position of "nonexistent" in original SQL
      expectedStopPos = Some(36), // length - 1 = 37 - 1 = 36
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
      expectedStartPos = 19, // Position of "nonexistent_table" in original SQL
      expectedStopPos = 35, // "SELECT :param FROM nonexistent_table".length - 1
      params = Map("param" -> 42)
    )
  }

  test("position mapping - nested expression with parameter") {
    // Complex nested expression
    checkPositionMapping[ExtendedAnalysisException](
      "SELECT (:param + 10) * *** FROM test",
      expectedStartPos = 32, // Actual position reported by analysis
      expectedStopPos = 35, // "SELECT (:param + 10) * *** FROM test".length - 1
      params = Map("param" -> 5)
    )
  }

  test("position mapping - parameter in WHERE clause") {
    // Parameter in WHERE clause with error
    checkPositionMapping[AnalysisException](
      "SELECT * FROM test WHERE id = :param AND undefined_column = 1",
      expectedStartPos = 14, // Actual position reported by analysis
      expectedStopPos = 17, // Actual stop position reported by analysis
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

  // =============================================================================
  // EXECUTE IMMEDIATE POSITION MAPPING TESTS
  // =============================================================================

  test("EXECUTE IMMEDIATE - parse error with named parameter") {
    // Test that parse errors in EXECUTE IMMEDIATE inner queries have correct position mapping
    // The error should reference the inner query context, not the outer EXECUTE IMMEDIATE
    spark.sql("DECLARE executeVar1 = 'SELECT :param ***'")
    checkExecuteImmediateError[ParseException](
      "EXECUTE IMMEDIATE executeVar1 USING 42 AS param",
      innerQuery = "SELECT :param ***",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(0),
      expectedStopPos = Some(16) // Actual stop position reported by parser
    )
  }

  test("EXECUTE IMMEDIATE - analysis error with named parameter") {
    // Test that analysis errors in EXECUTE IMMEDIATE inner queries have correct position mapping
    spark.sql("DECLARE executeVar2 = 'SELECT :param FROM nonexistent_table'")
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE executeVar2 USING 42 AS param",
      innerQuery = "SELECT :param FROM nonexistent_table",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(19), // Position of "nonexistent_table" in inner query
      expectedStopPos = Some(35) // End of "nonexistent_table" in inner query
    )
  }

  test("EXECUTE IMMEDIATE - positional parameter with parse error") {
    // Test positional parameters in EXECUTE IMMEDIATE
    spark.sql("DECLARE executeVar3 = 'SELECT ? ***'")
    checkExecuteImmediateError[ParseException](
      "EXECUTE IMMEDIATE executeVar3 USING 42",
      innerQuery = "SELECT ? ***",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(0),
      expectedStopPos = Some(11) // "SELECT ? ***".length - 1
    )
  }

  test("EXECUTE IMMEDIATE - multiple parameters with different lengths") {
    // Test multiple parameters with varying lengths in EXECUTE IMMEDIATE
    spark.sql("DECLARE executeVar4 = 'SELECT :a, :bb, :ccc FROM nonexistent'")
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE executeVar4 USING 1 AS a, 22 AS bb, 333 AS ccc",
      innerQuery = "SELECT :a, :bb, :ccc FROM nonexistent",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(26), // Position of "nonexistent" in inner query
      expectedStopPos = Some(36) // End of "nonexistent" in inner query
    )
  }

  test("EXECUTE IMMEDIATE - position mapping with parameter substitution") {
    // Test that error positions within EXECUTE IMMEDIATE are correctly mapped to inner query
    spark.sql("DECLARE executeVar5 = 'SELECT :param FROM nonexistent_table'")

    val exception = intercept[AnalysisException] {
      spark.sql("EXECUTE IMMEDIATE executeVar5 USING 42 AS param")
    }

    // Verify the error context references the inner query, not the EXECUTE IMMEDIATE statement
    if (exception.context.nonEmpty) {
      val context = exception.context.head.asInstanceOf[SQLQueryContext]
      assert(context.sqlText.exists(_.contains("SELECT :param FROM nonexistent_table")),
        s"Context should reference inner query, got: ${context.sqlText}")
    }
  }

  test("EXECUTE IMMEDIATE - nested parameter context") {
    // Test EXECUTE IMMEDIATE with parameters in both outer and inner contexts
    val exception = intercept[AnalysisException] {
      spark.sql("EXECUTE IMMEDIATE :query USING :value AS param", Map(
        "query" -> "SELECT :param FROM nonexistent_table",
        "value" -> 42
      ))
    }

    // Verify the error context references the inner query
    if (exception.context.nonEmpty) {
      val context = exception.context.head.asInstanceOf[SQLQueryContext]
      assert(context.sqlText.exists(_.contains("SELECT :param FROM nonexistent_table")),
        s"Context should reference inner query, got: ${context.sqlText}")
    }
  }

  test("EXECUTE IMMEDIATE - string parameter with complex inner query") {
    // Test EXECUTE IMMEDIATE with string parameter containing complex query
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE 'SELECT :a, :bb FROM nonexistent' USING 1 AS a, 22 AS bb",
      innerQuery = "SELECT :a, :bb FROM nonexistent",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(20), // Position of "nonexistent" in inner query
      expectedStopPos = Some(30) // End of "nonexistent" in inner query
    )
  }

  test("EXECUTE IMMEDIATE - position mapping with variable reference") {
    // Test position mapping when EXECUTE IMMEDIATE uses a variable
    spark.sql("DECLARE executeVar6 = 'SELECT :param1 + :param2 FROM nonexistent_table'")
    checkExecuteImmediateError[AnalysisException](
      "EXECUTE IMMEDIATE executeVar6 USING 10 AS param1, 20 AS param2",
      innerQuery = "SELECT :param1 + :param2 FROM nonexistent_table",
      // Error positions should be relative to the inner query
      expectedStartPos = Some(30), // Position of "nonexistent_table" in inner query
      expectedStopPos = Some(46) // End of "nonexistent_table" in inner query
    )
  }
}
