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
 * This feature works in all contexts where string literals are accepted,
 * not just in expressions.
 */
class StringLiteralCoalescingSuite extends QueryTest with SharedSparkSession {

  // ========================================================================
  // Basic String Literal Coalescing Tests
  // ========================================================================

  test("string coalescing - basic expressions") {
    val testCases = Seq(
      // Two literals
      ("SELECT 'hello' 'world'", "helloworld"),
      // Three literals
      ("SELECT 'one' 'two' 'three'", "onetwothree"),
      // Mixed quote styles: single and double
      ("SELECT 'hello' \"world\"", "helloworld"),
      // Mixed quote styles: multiple
      ("SELECT \"hello\" 'world' \"!\"", "helloworld!"),
      // Empty strings: start and end empty
      ("SELECT '' 'hello' ''", "hello"),
      // Empty strings: middle empty
      ("SELECT 'start' '' 'end'", "startend")
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
    }
  }

  // ========================================================================
  // DDL Context Coalescing Tests
  // ========================================================================

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

  // ========================================================================
  // LIKE and Pattern Matching Tests
  // ========================================================================

  test("string coalescing in LIKE patterns") {
    val testCases = Seq(
      // Coalescing with underscore wildcard - match
      ("SELECT 'test_value' LIKE 'test' '_value'", true),
      // Coalescing with underscore wildcard - no match
      ("SELECT 'test_value' LIKE 'test' '_xyz'", false),
      // Coalescing with percent wildcard
      ("SELECT 'prefix_middle_suffix' LIKE 'prefix' '%' 'suffix'", true),
      // ESCAPE clause with coalescing
      ("SELECT 'test%value' LIKE 'test' '\\%' 'value' ESCAPE '\\\\'", true)
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
    }
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
    val tableNames = Seq("test_table_123", "test_table_456", "other_table")
    withTable(tableNames: _*) {
      tableNames.foreach { tableName =>
        sql(s"CREATE TABLE $tableName (id INT)")
      }

      // The pattern is coalesced into 'test_table_*' (regex pattern where * matches any chars)
      // SHOW TABLES returns: namespace, tableName, isTemporary
      checkAnswer(
        sql("SHOW TABLES LIKE 'test' '_table_' '*'"),
        Seq(
          Row("default", "test_table_123", false),
          Row("default", "test_table_456", false)
        )
      )
    }
  }

  test("string coalescing across multiple lines") {
    val result = sql("""
      SELECT 'line'
             'one'
             'two'
    """).collect().head.getString(0)
    assert(result == "lineonetwo")
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

  // ========================================================================
  // R-String (Raw String) Coalescing Tests
  // ========================================================================

  test("R-string detection - basic cases and edge cases") {
    val testCases = Seq(
      // Basic cases: uppercase and lowercase R with single/double quotes
      ("""SELECT R'\n'""", raw"\n"),
      ("""SELECT r'\n'""", raw"\n"),
      ("""SELECT R"\n"""", raw"\n"),
      ("""SELECT r"\n"""", raw"\n"),
      // Edge cases: empty R-string
      ("""SELECT R''""", ""),
      // Quote character as content
      ("""SELECT R"'"""", "'"),
      ("""SELECT R'"'""", "\""),
      // Mixed escape sequences
      ("""SELECT R'\t\n\r\\'""", raw"\t\n\r\\"),
      // Backslashes at start and end
      ("""SELECT R'\test\'""", raw"""\test\""")
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
    }
  }

  test("R-string coalescing with regular strings") {
    val testCases = Seq(
      // R-string followed by regular string
      ("""SELECT R'\n' ' tab'""", raw"\n tab"),
      // Regular string followed by R-string
      ("""SELECT 'newline ' R'\n'""", raw"newline \n"),
      // Multiple R-strings with regular strings interleaved
      ("""SELECT R'\n' 'text' R'\t' 'more'""", raw"\ntext\tmore"),
      // All R-strings
      ("""SELECT R'\n' R'\t' R'\r'""", raw"\n\t\r")
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
    }
  }

  test("R-string detection - Windows paths") {
    val testCases = Seq(
      // Windows path with backslashes
      ("""SELECT R'C:\Users\JohnDoe\Documents\file.txt'""",
        raw"C:\Users\JohnDoe\Documents\file.txt"),
      // Coalesced Windows paths
      ("""SELECT R'C:\Users\' 'JohnDoe' R'\Documents'""",
        raw"C:\Users\JohnDoe\Documents"),
      // Mixed case R prefix with paths
      ("""SELECT r'C:\Windows\' R'System32'""",
        raw"C:\Windows\System32")
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
    }
  }

  test("R-string detection - special characters") {
    val testCases = Seq(
      // Dollar signs (should not be treated as escape)
      ("""SELECT R'$100\n'""", raw"$$100\n"),
      // Unicode-like sequences
      ("""SELECT R'\u0041'""", raw"\u0041"),
      // Regex patterns
      ("""SELECT R'\d+\.\d+'""", raw"\d+\.\d+"),
      // JSON-like content
      ("""SELECT R'{"key": "value\n"}'""", raw"""{"key": "value\n"}""")
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
    }
  }

  test("R-string coalescing - quote preservation") {
    val testCases = Seq(
      // Single-quoted R-strings coalesced
      ("""SELECT R'first' R'second'""", "firstsecond"),
      // Double-quoted R-strings coalesced
      ("""SELECT R"first" R"second"""", "firstsecond"),
      // Mixed single and double R-strings
      ("""SELECT R'first' R"second"""", "firstsecond"),
      // R-string with regular string (quote style from first non-R-string)
      ("""SELECT R'r-str' 'regular'""", "r-strregular")
    )

    testCases.foreach { case (query, expected) =>
      checkAnswer(sql(query), Row(expected))
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

  // ========================================================================
  // Property Coalescing Tests
  // ========================================================================

  test("property string coalescing - identifier keys") {
    withTable("t") {
      // Identifier key with value coalescing works with CREATE TABLE OPTIONS
      sql("""CREATE TABLE t (id INT) USING parquet
             OPTIONS(compression 'gzi' 'p')""")

      // Check that the option was set (look in catalog metadata)
      val props = spark.table("t").schema.json
      // The table was created successfully with coalesced value
      assert(sql("SHOW CREATE TABLE t").collect().head.getString(0).contains("gzip"))
    }
  }

  test("property string coalescing - string keys with equals") {
    withTable("t") {
      // String key with = and key coalescing
      sql("""CREATE TABLE t (id INT) USING parquet
             OPTIONS('my' '.' 'key' = 'value')""")

      val createStmt = sql("SHOW CREATE TABLE t").collect().head.getString(0)
      assert(createStmt.contains("my.key"))

      // String key with = and value coalescing
      withTable("t2") {
        sql("""CREATE TABLE t2 (id INT) USING parquet
               OPTIONS('compression' = 'sn' 'appy')""")

        val createStmt2 = sql("SHOW CREATE TABLE t2").collect().head.getString(0)
        assert(createStmt2.contains("snappy"))
      }
    }
  }

  test("property string coalescing - string keys without equals") {
    withTable("t") {
      // String key without = (key must be single token, value can coalesce)
      sql("""CREATE TABLE t (id INT) USING parquet
             OPTIONS('compression' 'snappy')""")

      val createStmt = sql("SHOW CREATE TABLE t").collect().head.getString(0)
      assert(createStmt.contains("compression"))
      assert(createStmt.contains("snappy"))

      // String key without = - multiple value tokens (should coalesce values)
      withTable("t2") {
        sql("""CREATE TABLE t2 (id INT) USING parquet
               OPTIONS('compression' 'sn' 'app' 'y')""")

        val createStmt2 = sql("SHOW CREATE TABLE t2").collect().head.getString(0)
        assert(createStmt2.contains("snappy"))
      }
    }
  }

  test("property string coalescing - CACHE TABLE") {
    // This was the original failing test case
    sql("CACHE LAZY TABLE cache_test AS SELECT 1 AS id")

    try {
      // String key without = should work (not coalesce key with value)
      sql("CACHE LAZY TABLE a OPTIONS('storageLevel' 'DISK_ONLY') AS SELECT 1")

      // Verify it's cached
      assert(spark.catalog.isCached("a"))

      // String key with = and value coalescing should also work
      sql("CACHE LAZY TABLE b OPTIONS('storageLevel' = 'MEMORY' '_ONLY') AS SELECT 1")
      assert(spark.catalog.isCached("b"))
    } finally {
      sql("UNCACHE TABLE IF EXISTS a")
      sql("UNCACHE TABLE IF EXISTS b")
      sql("UNCACHE TABLE IF EXISTS cache_test")
    }
  }

  test("property string coalescing - mixed property types") {
    withTable("t") {
      // Mix of different property key types in same OPTIONS clause
      sql("""CREATE TABLE t (id INT) USING parquet
             OPTIONS(
               compression 'snap' 'py',
               'my.prop' = 'val' 'ue',
               another_prop 'test' '123',
               'string.key' 'part1' 'part2'
             )""")

      val createStmt = sql("SHOW CREATE TABLE t").collect().head.getString(0)
      assert(createStmt.contains("snappy"))
      assert(createStmt.contains("value"))
      assert(createStmt.contains("test123"))
      assert(createStmt.contains("part1part2"))
    }
  }

  test("property string coalescing - R-strings in properties") {
    withTable("t") {
      // R-strings should work in property values
      // Use a valid option key instead of 'path' to avoid URI validation issues
      sql("""CREATE TABLE t (id INT) USING parquet
             OPTIONS('myoption' R'C:\Users\' 'data')""")

      val createStmt = sql("SHOW CREATE TABLE t").collect().head.getString(0)
      assert(createStmt.contains("""C:\Users\data"""))
    }
  }

  test("property string coalescing - TBLPROPERTIES") {
    withTable("t") {
      // TBLPROPERTIES uses propertyList (not expressionPropertyList)
      // Test string key with = and value coalescing
      sql("""CREATE TABLE t (id INT)
             TBLPROPERTIES('my' '.' 'key' = 'val' 'ue')""")

      val props = sql("SHOW TBLPROPERTIES t").collect()
      val propMap = props.map(r => r.getString(0) -> r.getString(1)).toMap
      assert(propMap.get("my.key").contains("value"))

      // Test string key without = (should not coalesce key with value)
      withTable("t2") {
        sql("""CREATE TABLE t2 (id INT)
               TBLPROPERTIES('another' 'test' '123')""")

        val props2 = sql("SHOW TBLPROPERTIES t2").collect()
        val propMap2 = props2.map(r => r.getString(0) -> r.getString(1)).toMap
        assert(propMap2.get("another").contains("test123"))
      }
    }
  }

  test("property string coalescing - parameter markers in OPTIONS") {
    withTable("t") {
      // Parameter marker as key (without =) - key is single token
      spark.sql(
        "CREATE TABLE t (id INT) USING parquet OPTIONS(:key 'value')",
        Map("key" -> "compression")
      )

      val createStmt = sql("SHOW CREATE TABLE t").collect().head.getString(0)
      assert(createStmt.contains("value"))

      // Parameter marker in value (without =)
      withTable("t2") {
        spark.sql(
          "CREATE TABLE t2 (id INT) USING parquet OPTIONS('compression' :value)",
          Map("value" -> "snappy")
        )

        val createStmt2 = sql("SHOW CREATE TABLE t2").collect().head.getString(0)
        assert(createStmt2.contains("snappy"))
      }

      // Parameter marker mixed with string literals in value (without =)
      withTable("t3") {
        spark.sql(
          "CREATE TABLE t3 (id INT) USING parquet OPTIONS('compression' :part1 'py')",
          Map("part1" -> "snap")
        )

        val createStmt3 = sql("SHOW CREATE TABLE t3").collect().head.getString(0)
        assert(createStmt3.contains("snappy"))
      }
    }
  }

  test("property string coalescing - UNSET TBLPROPERTIES with string key") {
    withTable("t") {
      // Test UNSET with string literal key (no value should be allowed)
      sql("CREATE TABLE t (id INT) TBLPROPERTIES('yes'='true')")
      sql("ALTER TABLE t UNSET TBLPROPERTIES('yes')")

      val props = sql("SHOW TBLPROPERTIES t").collect()
      val propMap = props.map(r => r.getString(0) -> r.getString(1)).toMap
      assert(!propMap.contains("yes"))
    }
  }

  test("property string coalescing - SHOW TBLPROPERTIES with coalesced key") {
    withTable("t") {
      // Test SHOW TBLPROPERTIES with coalesced string literal key
      sql("CREATE TABLE t (id INT) TBLPROPERTIES('my' 'Property'='value123')")

      // Should support coalescing in the SHOW command too
      val result = sql("SHOW TBLPROPERTIES t('my' 'Property')").collect()
      assert(result.length == 1)
      assert(result(0).getString(0) == "myProperty")
      assert(result(0).getString(1) == "value123")
    }
  }

  test("property string coalescing - parameter markers without equals in TBLPROPERTIES") {
    withTable("t") {
      // TBLPROPERTIES uses propertyList which supports keys without =
      // Parameter marker as key (without =)
      spark.sql(
        "CREATE TABLE t (id INT) TBLPROPERTIES(:key 'value')",
        Map("key" -> "my.property")
      )

      val props = sql("SHOW TBLPROPERTIES t").collect()
      val propMap = props.map(r => r.getString(0) -> r.getString(1)).toMap
      assert(propMap.get("my.property").contains("value"))

      // Parameter marker as value (without =)
      withTable("t2") {
        spark.sql(
          "CREATE TABLE t2 (id INT) TBLPROPERTIES('another' :value)",
          Map("value" -> "test123")
        )

        val props2 = sql("SHOW TBLPROPERTIES t2").collect()
        val propMap2 = props2.map(r => r.getString(0) -> r.getString(1)).toMap
        assert(propMap2.get("another").contains("test123"))
      }
    }
  }

  // ========================================================================
  // Edge Cases and Special Characters
  // ========================================================================

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

  // ========================================================================
  // Parameter Marker + String Literal Coalescing Tests
  // ========================================================================

  test("parameter marker with string literals - basic") {
    // Test mixing parameter markers with string literals
    checkAnswer(
      spark.sql("SELECT :param1 '/' :param2", Map("param1" -> "/data", "param2" -> "logs")),
      Row("/data/logs")
    )

    // Multiple literals around parameters
    checkAnswer(
      spark.sql("SELECT 'prefix' :mid 'suffix'", Map("mid" -> "_middle_")),
      Row("prefix_middle_suffix")
    )
  }

  test("parameter marker coalescing in LOCATION clause") {
    withTable("t") {
      withTempPath { dir =>
        val basePath = dir.getAbsolutePath
        spark.sql(
          "CREATE TABLE t (id INT) USING parquet LOCATION :base '/' :sub",
          Map("base" -> basePath, "sub" -> "data")
        )

        val location = spark.sessionState.catalog.getTableMetadata(
          spark.sessionState.sqlParser.parseTableIdentifier("t")).location
        assert(location.toString.contains(s"$basePath/data"))
      }
    }
  }

  test("parameter marker coalescing in COMMENT") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (id INT) COMMENT :prefix ': ' :desc",
        Map("prefix" -> "Table", "desc" -> "User data")
      )

      val comment = sql("DESCRIBE EXTENDED t")
        .filter("col_name = 'Comment'")
        .select("data_type")
        .collect()
        .head
        .getString(0)

      assert(comment == "Table: User data")
    }
  }

  test("parameter marker coalescing in column comments") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (
          |  id INT COMMENT :prefix ' - ' :desc
          |) USING parquet
        """.stripMargin,
        Map("prefix" -> "ID", "desc" -> "Primary key")
      )

      val columnComment = sql("DESCRIBE t")
        .filter("col_name = 'id'")
        .select("comment")
        .collect()
        .head
        .getString(0)

      assert(columnComment == "ID - Primary key")
    }
  }

  test("parameter marker coalescing in WHERE clause") {
    withTable("t") {
      sql("CREATE TABLE t (name STRING) USING parquet")
      sql("INSERT INTO t VALUES ('prefix_value'), ('prefix_other'), ('different')")

      checkAnswer(
        spark.sql("SELECT * FROM t WHERE name = :p1 '_' :p2",
          Map("p1" -> "prefix", "p2" -> "value")),
        Row("prefix_value")
      )
    }
  }

  test("parameter marker coalescing in LIKE patterns") {
    withTable("t") {
      sql("CREATE TABLE t (name STRING) USING parquet")
      sql("INSERT INTO t VALUES ('prefix_123'), ('prefix_456'), ('other')")

      val result = spark.sql(
        "SELECT * FROM t WHERE name LIKE :prefix '%'",
        Map("prefix" -> "prefix_")
      ).collect()

      assert(result.length == 2)
      assert(result.map(_.getString(0)).sorted === Array("prefix_123", "prefix_456"))
    }
  }

  test("parameter marker coalescing with empty strings") {
    checkAnswer(
      spark.sql("SELECT :p1 '' :p2", Map("p1" -> "", "p2" -> "hello")),
      Row("hello")
    )

    checkAnswer(
      spark.sql("SELECT 'start' :p1 'end'", Map("p1" -> "")),
      Row("startend")
    )
  }

  test("parameter marker coalescing - complex paths") {
    // Simulate building complex S3/HDFS paths
    checkAnswer(
      spark.sql(
        "SELECT :protocol '://' :bucket '/' :year '/' :month '/' :day '/' :file",
        Map(
          "protocol" -> "s3",
          "bucket" -> "my-bucket",
          "year" -> "2024",
          "month" -> "10",
          "day" -> "16",
          "file" -> "data.parquet"
        )
      ),
      Row("s3://my-bucket/2024/10/16/data.parquet")
    )
  }

  test("parameter marker coalescing across multiple lines") {
    val result = spark.sql(
      """SELECT :part1
        |       '/'
        |       :part2
        |       '/'
        |       :part3
      """.stripMargin,
      Map("part1" -> "a", "part2" -> "b", "part3" -> "c")
    ).collect().head.getString(0)

    assert(result == "a/b/c")
  }

  test("parameter marker coalescing with special characters") {
    checkAnswer(
      spark.sql("SELECT :p1 '\\t' :p2", Map("p1" -> "hello", "p2" -> "world")),
      Row("hello\tworld")
    )

    checkAnswer(
      spark.sql("SELECT :p1 '\\n' :p2", Map("p1" -> "line1", "p2" -> "line2")),
      Row("line1\nline2")
    )
  }

  test("parameter marker - consecutive parameters with literals") {
    // Test with consecutive parameter markers mixed with string literals
    checkAnswer(
      spark.sql("SELECT :p1 '' :p2 '' :p3", Map("p1" -> "a", "p2" -> "b", "p3" -> "c")),
      Row("abc")
    )
  }

  test("parameter marker coalescing with R-strings") {
    // R-strings with parameters
    checkAnswer(
      spark.sql("""SELECT R'C:\Users\' :username R'\Documents'""",
        Map("username" -> "JohnDoe")),
      Row("""C:\Users\JohnDoe\Documents""")
    )

    // Mix parameter with R-string and regular string
    checkAnswer(
      spark.sql("""SELECT :prefix R'\path\to\file'""", Map("prefix" -> "Location: ")),
      Row("""Location: \path\to\file""")
    )
  }

  test("parameter marker - positional only") {
    // Test with only positional parameters
    checkAnswer(
      spark.sql("SELECT ? '/' ?", Array("first", "second")),
      Row("first/second")
    )
  }

  test("parameter marker coalescing in INSERT VALUES") {
    withTable("t") {
      sql("CREATE TABLE t (path STRING) USING parquet")
      spark.sql(
        "INSERT INTO t VALUES (:base '/' :file)",
        Map("base" -> "/data", "file" -> "file.txt")
      )

      checkAnswer(
        sql("SELECT * FROM t"),
        Row("/data/file.txt")
      )
    }
  }

  test("parameter marker coalescing with INTERVAL") {
    // YEAR TO MONTH intervals use 'y-m' format
    checkAnswer(
      spark.sql("SELECT INTERVAL '10' :sep '1' YEAR TO MONTH",
        Map("sep" -> "-")),
      sql("SELECT INTERVAL '10-1' YEAR TO MONTH")
    )
  }
}
