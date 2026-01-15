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

package org.apache.spark.sql.scripting

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Comprehensive Scala test suite for SQL Scripting cursor functionality.
 * This replaces cursors.sql with inline expected output validation.
 */
class SqlScriptingCursorSuite extends QueryTest with SharedSparkSession {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    conf.setConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED, true)
    conf.setConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED, true)
  }

  protected override def afterAll(): Unit = {
    conf.unsetConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED.key)
    conf.unsetConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key)
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  // Test 0a-0d: Feature disabled
  test("DECLARE CURSOR disabled by default") {
    withSQLConf(SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SqlScriptingException] {
          spark.sql("BEGIN DECLARE cur CURSOR FOR SELECT 1; END")
        },
        condition = "UNSUPPORTED_FEATURE.SQL_CURSOR",
        parameters = Map.empty
      )
    }
  }

  // Test 1: Basic cursor operations
  test("Basic cursor lifecycle") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT;
        DECLARE cur CURSOR FOR SELECT 42 AS val;
        OPEN cur;
        FETCH cur INTO x;
        CLOSE cur;
        VALUES (x);
      END
    """)
    checkAnswer(result, Seq(Row(42)))
  }

  // Test 2a: Cursor cannot be opened twice
  test("Cursor cannot be opened twice") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("""
          BEGIN
            DECLARE cur CURSOR FOR SELECT 1;
            OPEN cur;
            OPEN cur;
          END
        """)
      },
      condition = "CURSOR_ALREADY_OPEN",
      parameters = Map("cursorName" -> "`cur`")
    )
  }

  // Test 2b: Cursor can be closed and re-opened
  test("Cursor can be closed and re-opened") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT;
        DECLARE cur CURSOR FOR SELECT 42 AS val;
        OPEN cur;
        FETCH cur INTO x;
        CLOSE cur;
        OPEN cur;
        FETCH cur INTO x;
        CLOSE cur;
        VALUES (x);
      END
    """)
    checkAnswer(result, Seq(Row(42)))
  }

  // Test 34: CURSOR_NO_MORE_ROWS is a completion condition - continues execution
  test("CURSOR_NO_MORE_ROWS continues execution (completion condition)") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT DEFAULT 0;
        DECLARE result STRING DEFAULT '';
        DECLARE cur CURSOR FOR SELECT 42 AS val;

        OPEN cur;
        FETCH cur INTO x;
        SET result = result || CAST(x AS STRING);

        FETCH cur INTO x;  -- CURSOR_NO_MORE_ROWS (02000) - continues execution

        SET result = result || '-after-fetch';
        CLOSE cur;

        VALUES (result);
      END
    """)
    checkAnswer(result, Seq(Row("42-after-fetch")))
  }

  // Test 35: NOT FOUND handler catches CURSOR_NO_MORE_ROWS
  test("NOT FOUND handler catches CURSOR_NO_MORE_ROWS") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT DEFAULT 0;
        DECLARE cur CURSOR FOR SELECT 1;

        DECLARE CONTINUE HANDLER FOR NOT FOUND
          SET x = 999;

        OPEN cur;
        FETCH cur INTO x;
        FETCH cur INTO x;
        CLOSE cur;

        VALUES (x);
      END
    """)
    checkAnswer(result, Seq(Row(999)))
  }

  // Test 36: EXIT HANDLER for NOT FOUND - exits block, no VALUES executed
  test("EXIT HANDLER for NOT FOUND exits block") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT DEFAULT 0;
        DECLARE cur CURSOR FOR SELECT 1;

        DECLARE EXIT HANDLER FOR NOT FOUND
          SET x = 777;

        OPEN cur;
        FETCH cur INTO x;
        FETCH cur INTO x;

        SET x = 888;
        CLOSE cur;
        VALUES (x);
      END
    """)
    // EXIT handler exits the block before VALUES - script returns empty
    assert(result.collect().isEmpty)
  }

  // Test 4: Parameterized cursor
  test("Parameterized cursor with positional parameters") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT;
        DECLARE cur CURSOR FOR SELECT ? + ? AS result;
        OPEN cur USING 10, 20;
        FETCH cur INTO x;
        CLOSE cur;
        VALUES (x);
      END
    """)
    checkAnswer(result, Seq(Row(30)))
  }

  // Test 40: Nested cursors
  test("Multiple cursors in sequence") {
    val result = spark.sql("""
      BEGIN
        DECLARE sum INT DEFAULT 0;
        DECLARE temp INT;
        DECLARE cur1 CURSOR FOR SELECT 10 AS val;
        DECLARE cur2 CURSOR FOR SELECT 20 AS val;
        DECLARE cur3 CURSOR FOR SELECT 30 AS val;

        OPEN cur1;
        FETCH cur1 INTO temp;
        SET sum = sum + temp;
        CLOSE cur1;

        OPEN cur2;
        FETCH cur2 INTO temp;
        SET sum = sum + temp;
        CLOSE cur2;

        OPEN cur3;
        FETCH cur3 INTO temp;
        SET sum = sum + temp;
        CLOSE cur3;

        VALUES (sum);
      END
    """)
    checkAnswer(result, Seq(Row(60)))
  }

  // Test: CONTINUE HANDLER in REPEAT loop
  test("CONTINUE HANDLER in REPEAT loop") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT DEFAULT 0;
        DECLARE iterations INT DEFAULT 0;

        DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
          SET x = 100;

        REPEAT
          SET iterations = iterations + 1;
          IF iterations = 2 THEN
            SELECT 1 / 0;
          END IF;
        UNTIL iterations >= 3
        END REPEAT;

        VALUES (x, iterations);
      END
    """)
    checkAnswer(result, Seq(Row(100, 2)))
  }

  // Test: Cross-frame cursor access
  test("CONTINUE HANDLER accessing outer cursor") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT DEFAULT 0;
        DECLARE cur CURSOR FOR SELECT 99;

        DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
        BEGIN
          FETCH cur INTO x;
          CLOSE cur;
        END;

        OPEN cur;
        SELECT 1 / 0;
        VALUES (x);
      END
    """)
    checkAnswer(result, Seq(Row(99)))
  }

  // Test 67: Declaration order - cursor before variable
  test("Declaration order validation - cursor before variable not allowed") {
    val exception = intercept[Exception] {
      spark.sql("""
        BEGIN
          DECLARE cur CURSOR FOR SELECT 1;
          DECLARE x INT;
        END
      """)
    }
    // Check that it's a parse or analysis error about declaration order
    assert(exception.getMessage.contains("VARIABLE") ||
           exception.getMessage.contains("CURSOR") ||
           exception.getMessage.contains("BEGINNING"))
  }

  // Test: Cursor with no rows
  test("Cursor with empty result set") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT DEFAULT -1;
        DECLARE cur CURSOR FOR SELECT 1 WHERE FALSE;

        DECLARE CONTINUE HANDLER FOR NOT FOUND
          SET x = 0;

        OPEN cur;
        FETCH cur INTO x;
        CLOSE cur;

        VALUES (x);
      END
    """)
    checkAnswer(result, Seq(Row(0)))
  }

  // Test: Multiple cursors
  test("Multiple cursors in same scope") {
    val result = spark.sql("""
      BEGIN
        DECLARE x INT;
        DECLARE y INT;
        DECLARE cur1 CURSOR FOR SELECT 10;
        DECLARE cur2 CURSOR FOR SELECT 20;

        OPEN cur1;
        OPEN cur2;
        FETCH cur1 INTO x;
        FETCH cur2 INTO y;
        CLOSE cur1;
        CLOSE cur2;

        VALUES (x + y);
      END
    """)
    checkAnswer(result, Seq(Row(30)))
  }

  // Test: FETCH INTO STRUCT
  test("FETCH INTO STRUCT variable") {
    val result = spark.sql("""
      BEGIN
        DECLARE v STRUCT<a: INT, b: STRING>;
        DECLARE cur CURSOR FOR SELECT 1 AS a, 'hello' AS b;
        OPEN cur;
        FETCH cur INTO v;
        CLOSE cur;
        VALUES (v);
      END
    """)
    checkAnswer(result, Seq(Row(Row(1, "hello"))))
  }

  // Test: Cursor iteration with loop
  test("Cursor iteration with REPEAT and handler") {
    val result = spark.sql("""
      BEGIN
        DECLARE sum INT DEFAULT 0;
        DECLARE val INT;
        DECLARE done INT DEFAULT 0;
        DECLARE cur CURSOR FOR SELECT * FROM VALUES (1), (2), (3) AS t(x);

        DECLARE CONTINUE HANDLER FOR NOT FOUND
          SET done = 1;

        OPEN cur;

        REPEAT
          FETCH cur INTO val;
          IF done = 0 THEN
            SET sum = sum + val;
          END IF;
        UNTIL done = 1
        END REPEAT;

        CLOSE cur;
        VALUES (sum);
      END
    """)
    checkAnswer(result, Seq(Row(6)))
  }
}
