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

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for string literal coalescing across all SQL grammar contexts.
 *
 * String literal coalescing allows multiple consecutive string literals to be
 * automatically concatenated: 'hello' 'world' becomes 'helloworld'.
 *
 * This feature now works in all contexts where string literals are accepted,
 * not just in expressions.
 */
class StringLiteralCoalescingSuite extends QueryTest with SharedSparkSession {

  test("string coalescing in SELECT expressions") {
    checkAnswer(
      sql("SELECT 'hello' 'world'"),
      Row("helloworld")
    )

    checkAnswer(
      sql("SELECT 'one' 'two' 'three'"),
      Row("onetwothree")
    )
  }

  test("string coalescing with mixed quote styles") {
    checkAnswer(
      sql("SELECT 'hello' \"world\""),
      Row("helloworld")
    )

    checkAnswer(
      sql("SELECT \"hello\" 'world' \"!\""),
      Row("helloworld!")
    )
  }

  test("string coalescing in DDL LOCATION clause") {
    withTempPath { dir =>
      withTable("t") {
        val path1 = dir.getAbsolutePath
        val path2 = "/test_data"
        sql(s"CREATE TABLE t (id INT) USING parquet LOCATION '$path1' '$path2'")

        val location = sql("DESCRIBE EXTENDED t")
          .filter("col_name = 'Location'")
          .select("data_type")
          .collect()
          .head
          .getString(0)

        assert(location.contains(path1 + path2))
      }
    }
  }

  test("string coalescing in DDL COMMENT clause") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) COMMENT 'This is ' 'a multi' 'part comment'")

      val comment = sql("DESCRIBE EXTENDED t")
        .filter("col_name = 'Comment'")
        .select("data_type")
        .collect()
        .head
        .getString(0)

      assert(comment == "This is a multipart comment")
    }
  }

  test("string coalescing in column COMMENT") {
    withTable("t") {
      sql("CREATE TABLE t (id INT COMMENT 'User ' 'ID' ' number')")

      val comment = sql("DESCRIBE t")
        .filter("col_name = 'id'")
        .select("comment")
        .collect()
        .head
        .getString(0)

      assert(comment == "User ID number")
    }
  }

  test("string coalescing in LIKE pattern") {
    checkAnswer(
      sql("SELECT 'test_value' LIKE 'test' '_value'"),
      Row(true)
    )

    checkAnswer(
      sql("SELECT 'test_value' LIKE 'test' '_xyz'"),
      Row(false)
    )

    checkAnswer(
      sql("SELECT 'prefix_middle_suffix' LIKE 'prefix' '%' 'suffix'"),
      Row(true)
    )
  }

  test("string coalescing in ESCAPE clause") {
    // Test escape character coalescing (though typically escape is single char)
    checkAnswer(
      sql("SELECT 'test%value' LIKE 'test' '\\%' 'value' ESCAPE '\\\\'"),
      Row(true)
    )
  }

  test("string coalescing in table options") {
    withTable("t") {
      withTempPath { dir =>
        val path1 = s"${dir.getAbsolutePath}/part"
        val path2 = "1"
        // Test that LOCATION paths can be coalesced
        sql(s"CREATE TABLE t (a STRING, b STRING) USING parquet LOCATION '$path1' '$path2'")
        assert(spark.catalog.tableExists("t"))
        val location = spark.sessionState.catalog.getTableMetadata(
          spark.sessionState.sqlParser.parseTableIdentifier("t")).location
        assert(location.toString.contains("part1"))
      }
    }
  }

  test("string coalescing in SHOW TABLES LIKE pattern") {
    withTable("test_table_123", "test_table_456", "other_table") {
      sql("CREATE TABLE test_table_123 (id INT)")
      sql("CREATE TABLE test_table_456 (id INT)")
      sql("CREATE TABLE other_table (id INT)")

      // The pattern is coalesced into 'test_table_*' (regex pattern where * matches any chars)
      val result = sql("SHOW TABLES LIKE 'test' '_table_' '*'").collect()
      assert(result.length == 2,
        s"Expected 2 tables, got ${result.length}: ${result.mkString(", ")}")
      val tableNames = result.map(_.getString(1)).sorted
      assert(tableNames === Array("test_table_123", "test_table_456"))
    }
  }

  test("type constructor with string coalescing") {
    // DATE type constructor should support coalescing
    checkAnswer(
      sql("SELECT DATE '2023' '-10' '-16'"),
      Row(java.sql.Date.valueOf("2023-10-16"))
    )

    // TIMESTAMP type constructor should support coalescing
    checkAnswer(
      sql("SELECT TIMESTAMP '2023-10-16' ' 12:30:00'"),
      Row(java.sql.Timestamp.valueOf("2023-10-16 12:30:00"))
    )

    // INTERVAL type constructor should support coalescing
    checkAnswer(
      sql("SELECT INTERVAL '10' '-1' YEAR TO MONTH"),
      sql("SELECT INTERVAL '10-1' YEAR TO MONTH")
    )

    checkAnswer(
      sql("SELECT INTERVAL '1' ' 12:30:45.678' DAY TO SECOND"),
      sql("SELECT INTERVAL '1 12:30:45.678' DAY TO SECOND")
    )

    // Binary hex literal (X'...') should support coalescing
    checkAnswer(
      sql("SELECT X'DE' 'AD' 'BE' 'EF'"),
      Row(Array[Byte](0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte))
    )
  }

  test("string coalescing across multiple lines") {
    val result = sql("""
      SELECT 'line'
             'one'
             'two'
    """).collect().head.getString(0)
    assert(result == "lineonetwo")
  }

  test("string coalescing with empty strings") {
    checkAnswer(
      sql("SELECT '' 'hello' ''"),
      Row("hello")
    )

    checkAnswer(
      sql("SELECT 'start' '' 'end'"),
      Row("startend")
    )
  }

  test("string coalescing in WHERE clause") {
    withTable("t") {
      sql("CREATE TABLE t (name STRING) USING parquet")
      sql("INSERT INTO t VALUES ('helloworld'), ('hello'), ('world')")

      checkAnswer(
        sql("SELECT * FROM t WHERE name = 'hello' 'world'"),
        Row("helloworld")
      )
    }
  }

  test("string coalescing in INSERT VALUES") {
    withTable("t") {
      sql("CREATE TABLE t (name STRING) USING parquet")
      sql("INSERT INTO t VALUES ('hello' 'world')")

      checkAnswer(
        sql("SELECT * FROM t"),
        Row("helloworld")
      )
    }
  }

  test("string coalescing with COMMENT ON TABLE") {
    withTable("t") {
      sql("CREATE TABLE t (id INT)")
      sql("COMMENT ON TABLE t IS 'Updated' ' comment' ' text'")

      val comment = sql("DESCRIBE EXTENDED t")
        .filter("col_name = 'Comment'")
        .select("data_type")
        .collect()
        .head
        .getString(0)

      assert(comment == "Updated comment text")
    }
  }

  test("string coalescing preserves whitespace within literals") {
    checkAnswer(
      sql("SELECT 'hello  ' '  world'"),
      Row("hello    world")
    )
  }

  test("string coalescing with special characters") {
    // Test that special characters are preserved correctly during coalescing
    checkAnswer(
      sql("SELECT 'tab:\\t' 'newline:\\n' 'end'"),
      Row("tab:\tnewline:\nend")
    )

    // Test escaped single quote
    checkAnswer(
      sql("SELECT 'it''s' ' a' ' test'"),
      Row("it's a test")
    )
  }

  test("string coalescing does not affect single literals") {
    // Ensure single literals still work correctly (fast path)
    checkAnswer(
      sql("SELECT 'single'"),
      Row("single")
    )

    withTable("t") {
      sql("CREATE TABLE t (id INT) COMMENT 'single'")
      val comment = sql("DESCRIBE EXTENDED t")
        .filter("col_name = 'Comment'")
        .select("data_type")
        .collect()
        .head
        .getString(0)
      assert(comment == "single")
    }
  }
}
