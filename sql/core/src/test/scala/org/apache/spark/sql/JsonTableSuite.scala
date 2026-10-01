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

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, TimestampType}

/**
 * End-to-end tests for the SQL:2016 `JSON_TABLE` table-valued function (flat, non-nested subset).
 */
class JsonTableSuite extends QueryTest with SharedSparkSession {

  test("expand a JSON array into rows with typed columns and ordinality") {
    val json = """{"items":[{"id":1,"n":"a"},{"id":2,"n":"b"},{"id":3,"n":"c"}]}"""
    val df = sql(
      s"""
         |SELECT t.* FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (
         |    seq  FOR ORDINALITY,
         |    id   INT    PATH '$$.id',
         |    name STRING PATH '$$.n'
         |  )
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(1L, 1, "a"), Row(2L, 2, "b"), Row(3L, 3, "c")))
    // Ordinality is a BIGINT, typed columns take their declared types.
    assert(df.schema.map(_.dataType) === Seq(LongType, IntegerType, StringType))
  }

  test("implicit column path derived from column name") {
    val json = """{"rows":[{"id":10,"name":"x"},{"id":20,"name":"y"}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.rows[*]'
         |  COLUMNS (id INT, name STRING)
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(10, "x"), Row(20, "y")))
  }

  test("row path matching a single object yields one row") {
    val json = """{"a":1,"b":"hello"}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$'
         |  COLUMNS (a INT PATH '$$.a', b STRING PATH '$$.b')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(1, "hello")))
  }

  test("missing column path yields null") {
    val json = """{"items":[{"id":1},{"id":2,"n":"b"}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (id INT PATH '$$.id', name STRING PATH '$$.n')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(1, null), Row(2, "b")))
  }

  test("EXISTS column reports presence as boolean") {
    val json = """{"items":[{"id":1,"opt":9},{"id":2}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (id INT PATH '$$.id', hasOpt BOOLEAN EXISTS PATH '$$.opt')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(1, true), Row(2, false)))
  }

  test("empty array yields no rows") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[]}',
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq.empty)
  }

  test("row path matching nothing yields no rows") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"id":1}]}',
        |  '$.absent[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq.empty)
  }

  test("NULL ON ERROR (default) yields no rows for malformed JSON") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{ this is not valid json',
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq.empty)

    // Explicit NULL ON ERROR behaves the same.
    val df2 = sql(
      """
        |SELECT * FROM json_table(
        |  '{ this is not valid json',
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id')
        |  NULL ON ERROR
        |) AS t
      """.stripMargin)
    checkAnswer(df2, Seq.empty)
  }

  test("ERROR ON ERROR raises for malformed JSON") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{ this is not valid json',
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id')
        |  ERROR ON ERROR
        |) AS t
      """.stripMargin)
    intercept[SparkException] {
      df.collect()
    }
  }

  test("null JSON input yields no rows") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  CAST(NULL AS STRING),
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq.empty)
  }

  test("untyped NULL input is coerced and yields no rows (NULL ON ERROR)") {
    // An untyped SQL NULL (NullType) must be coerced to STRING and apply NULL ON ERROR, not be
    // rejected during analysis.
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  NULL,
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq.empty)
  }

  test("join JSON_TABLE output against a base table") {
    import testImplicits._
    withTempView("docs") {
      Seq(
        (1, """{"tags":[{"k":"a"},{"k":"b"}]}"""),
        (2, """{"tags":[{"k":"c"}]}""")
      ).toDF("id", "doc").createOrReplaceTempView("docs")

      val df = sql(
        """
          |SELECT d.id, t.k
          |FROM docs d,
          |LATERAL json_table(d.doc, '$.tags[*]' COLUMNS (k STRING PATH '$.k')) AS t
          |ORDER BY d.id, t.k
        """.stripMargin)
      checkAnswer(df, Seq(Row(1, "a"), Row(1, "b"), Row(2, "c")))
    }
  }

  test("nested field extraction within a row item") {
    val json = """{"items":[{"meta":{"score":7}},{"meta":{"score":8}}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (score INT PATH '$$.meta.score')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(7), Row(8)))
  }

  test("column path that is a prefix of another is resolved correctly") {
    // `$.meta` both terminates a column and is a prefix of `$.meta.score` and `$.meta.name`, so
    // all three (plus an EXISTS on the prefix) must resolve from the same item.
    val json = """{"items":[{"meta":{"score":7,"name":"a"}},{"meta":{"score":8,"name":"b"}}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (
         |    meta STRING PATH '$$.meta',
         |    has_meta BOOLEAN EXISTS PATH '$$.meta',
         |    score INT PATH '$$.meta.score',
         |    name STRING PATH '$$.meta.name')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(
      Row("""{"score":7,"name":"a"}""", true, 7, "a"),
      Row("""{"score":8,"name":"b"}""", true, 8, "b")))
  }

  test("ordinality-only table produces one numbered row per array element") {
    // No path columns: every element still yields a row, numbered by ordinality, even when the
    // element itself is a scalar (the item is never inspected for a value).
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[10,20,30]}',
        |  '$.items[*]'
        |  COLUMNS (seq FOR ORDINALITY)
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row(1L), Row(2L), Row(3L)))
  }

  test("column alias list from the table alias") {
    val json = """{"items":[{"id":1,"n":"a"}]}"""
    val df = sql(
      s"""
         |SELECT renamed_id, renamed_name FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (id INT PATH '$$.id', name STRING PATH '$$.n')
         |) AS t(renamed_id, renamed_name)
       """.stripMargin)
    checkAnswer(df, Seq(Row(1, "a")))
  }

  test("duplicate column names are rejected at parse time") {
    val e = intercept[ParseException] {
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"items":[{"id":1}]}',
          |  '$.items[*]'
          |  COLUMNS (id INT PATH '$.id', id STRING PATH '$.id')
          |) AS t
        """.stripMargin)
    }
    assert(e.getCondition == "INVALID_SQL_SYNTAX.DUPLICATE_JSON_TABLE_COLUMN")
    assert(e.getMessageParameters.get("columnName") == "`id`")
  }

  test("duplicate column name detection follows spark.sql.caseSensitive") {
    // Names differing only in case: distinct under a case-sensitive resolver, colliding otherwise.
    val mixedCase =
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"id":1,"ID":2}]}',
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id', ID INT PATH '$.ID')
        |) AS t
      """.stripMargin
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(sql(mixedCase), Seq(Row(1, 2)))
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val e = intercept[ParseException](sql(mixedCase))
      assert(e.getCondition == "INVALID_SQL_SYNTAX.DUPLICATE_JSON_TABLE_COLUMN")
      assert(e.getMessageParameters.get("columnName") == "`id`")
    }
    // An exact-case duplicate is rejected in both modes.
    val exactDuplicate =
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"id":1}]}',
        |  '$.items[*]'
        |  COLUMNS (id INT PATH '$.id', id INT PATH '$.id')
        |) AS t
      """.stripMargin
    Seq("true", "false").foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        val e = intercept[ParseException](sql(exactDuplicate))
        assert(e.getCondition == "INVALID_SQL_SYNTAX.DUPLICATE_JSON_TABLE_COLUMN")
      }
    }
  }

  test("value cast honors ANSI mode") {
    val json = """{"items":[{"v":"not_a_number"}]}"""
    val query =
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (v INT PATH '$$.v')
         |) AS t
       """.stripMargin
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkAnswer(sql(query), Seq(Row(null)))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // ANSI cast failures surface as a SparkThrowable (e.g. SparkNumberFormatException).
      intercept[SparkThrowable] {
        sql(query).collect()
      }
    }
  }

  test("EXISTS distinguishes a present JSON null from a missing key") {
    // A key present with a JSON null value EXISTS (true); a truly absent key does not (false).
    val json = """{"items":[{"a":null},{"b":1}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (hasA BOOLEAN EXISTS PATH '$$.a')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(true), Row(false)))
  }

  test("value column returns SQL NULL (not the string 'null') for a JSON null") {
    // JSON null must become SQL NULL, while a JSON string "null" must remain the string.
    val json = """{"items":[{"v":null},{"v":"null"},{"v":"x"}]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (v STRING PATH '$$.v')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(null), Row("null"), Row("x")))
  }

  test("value column returns SQL NULL for a JSON null reached via an array index") {
    val json = """{"arr":[null, "kept"]}"""
    val df = sql(
      s"""
         |SELECT * FROM json_table(
         |  '$json',
         |  '$$'
         |  COLUMNS (first STRING PATH '$$.arr[0]', second STRING PATH '$$.arr[1]')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(null, "kept")))
  }

  test("mid-path wildcard in a column or row path is rejected") {
    // A wildcard anywhere except a single trailing '[*]' on the row path is unsupported.
    val e1 = intercept[AnalysisException] {
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"items":[{"x":1}]}',
          |  '$.items[*].x'
          |  COLUMNS (x INT PATH '$.x')
          |) AS t
        """.stripMargin).collect()
    }
    assert(e1.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_TABLE_PATH")
    assert(e1.getMessageParameters.get("location") == "row path")
    assert(e1.getMessageParameters.get("path") == "'$.items[*].x'")
    // The rendered expression carries the full JSON_TABLE syntax (row path + columns + ON ERROR),
    // not just the JSON input, so the diagnostic is actionable.
    assert(e1.getMessage.contains(
      "JSON_TABLE({\"items\":[{\"x\":1}]}, '$.items[*].x' " +
        "COLUMNS (x INT PATH '$.x') NULL ON ERROR)"))

    val e2 = intercept[AnalysisException] {
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"items":[{"vals":[1,2]}]}',
          |  '$.items[*]'
          |  COLUMNS (v INT PATH '$.vals[*]')
          |) AS t
        """.stripMargin).collect()
    }
    assert(e2.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_TABLE_PATH")
    assert(e2.getMessageParameters.get("location") == "column 'v'")
    assert(e2.getMessageParameters.get("path") == "'$.vals[*]'")
  }

  test("[*] over a non-array fires ERROR ON ERROR and is empty under NULL ON ERROR") {
    val json = """{"items":{"a":1}}"""
    // NULL ON ERROR (default): a non-array under [*] yields no rows.
    checkAnswer(
      sql(
        s"""
           |SELECT * FROM json_table(
           |  '$json', '$$.items[*]' COLUMNS (a INT PATH '$$.a')
           |) AS t
         """.stripMargin),
      Seq.empty)
    // ERROR ON ERROR: the same input raises.
    intercept[SparkException] {
      sql(
        s"""
           |SELECT * FROM json_table(
           |  '$json', '$$.items[*]' COLUMNS (a INT PATH '$$.a') ERROR ON ERROR
           |) AS t
         """.stripMargin).collect()
    }
  }

  test("non-explode row path resolving to a string value") {
    // The matched row item is a JSON string; a value column reading '$' must get its content.
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"name":"hello world"}',
        |  '$.name'
        |  COLUMNS (c STRING PATH '$', present BOOLEAN EXISTS PATH '$')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row("hello world", true)))
  }

  test("row path resolving to a top-level string") {
    val df = sql(
      """
        |SELECT * FROM json_table('"just a string"', '$' COLUMNS (c STRING PATH '$')) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row("just a string")))
  }

  test("array of scalars as the row source") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"nums":[1,2,3]}',
        |  '$.nums[*]'
        |  COLUMNS (seq FOR ORDINALITY, v INT PATH '$')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row(1L, 1), Row(2L, 2), Row(3L, 3)))
  }

  test("high-precision fractional value is preserved exactly, not rounded to a double") {
    // The matched fragment is reserialized before JSON_TABLE casts it. The number carries more
    // significant digits than a double can hold, so serializing via a lossy float copy would round
    // it before the DECIMAL/STRING cast sees it. Exact structure copying keeps the digits verbatim.
    val json = """{"v":123456789.123456789123456789}"""
    val df = sql(
      s"""
        |SELECT * FROM json_table(
        |  '$json',
        |  '$$'
        |  COLUMNS (d DECIMAL(38, 18) PATH '$$.v', s STRING PATH '$$.v')
        |) AS t
      """.stripMargin)
    checkAnswer(df,
      Seq(Row(BigDecimal("123456789.123456789123456789"), "123456789.123456789123456789")))
  }

  test("array of strings as the row source keeps string content") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"tags":["a","b c"]}',
        |  '$.tags[*]'
        |  COLUMNS (v STRING PATH '$')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row("a"), Row("b c")))
  }

  test("duplicate keys within an item resolve to the first value") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"a":1,"a":2}]}',
        |  '$.items[*]'
        |  COLUMNS (a INT PATH '$.a')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row(1)))
  }

  test("structure-valued column serialized to STRING") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"o":{"x":1}}]}',
        |  '$.items[*]'
        |  COLUMNS (o STRING PATH '$.o')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row("""{"x":1}""")))
  }

  test("unsupported declared column type is rejected at analysis") {
    // A value column cannot be declared as a complex type that STRING cannot be cast to.
    val e = intercept[AnalysisException] {
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"items":[{"v":1}]}',
          |  '$.items[*]'
          |  COLUMNS (v STRUCT<a: INT> PATH '$.v')
          |) AS t
        """.stripMargin).collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION")
  }

  test("oversized numeric path index is rejected as an invalid path") {
    val e = intercept[AnalysisException] {
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"a":[1]}',
          |  '$'
          |  COLUMNS (v INT PATH '$.a[999999999999999999999999]')
          |) AS t
        """.stripMargin).collect()
    }
    assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_JSON_TABLE_PATH")
  }

  test("ERROR ON ERROR rejects trailing garbage and empty input") {
    // A valid JSON prefix followed by garbage must raise, not silently produce rows.
    intercept[SparkException] {
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"items":[{"id":1}]} trailing garbage',
          |  '$.items[*]' COLUMNS (id INT PATH '$.id') ERROR ON ERROR
          |) AS t
        """.stripMargin).collect()
    }
    // Empty input is malformed under ERROR ON ERROR.
    intercept[SparkException] {
      sql(
        """
          |SELECT * FROM json_table('', '$.items[*]' COLUMNS (id INT PATH '$.id') ERROR ON ERROR)
          |AS t
        """.stripMargin).collect()
    }
    // Under the default NULL ON ERROR the same inputs simply yield no rows.
    checkAnswer(
      sql(
        """
          |SELECT * FROM json_table(
          |  '{"items":[{"id":1}]} trailing garbage',
          |  '$.items[*]' COLUMNS (id INT PATH '$.id')
          |) AS t
        """.stripMargin),
      Seq.empty)
  }

  test("large array row source is expanded correctly") {
    // A big array is fully and correctly expanded. Row emission is streamed element by element
    // from the source parser (the whole expanded payload is not materialized at once); note the
    // input is still scanned once up front to validate it is a single well-formed JSON document.
    val n = 5000
    val arr = (1 to n).map(i => s"""{"id":$i}""").mkString(",")
    val json = s"""{"items":[$arr]}"""
    val df = sql(
      s"""
         |SELECT count(*) AS c, sum(id) AS s FROM json_table(
         |  '$json',
         |  '$$.items[*]'
         |  COLUMNS (id INT PATH '$$.id')
         |) AS t
       """.stripMargin)
    checkAnswer(df, Seq(Row(n.toLong, (n.toLong * (n + 1)) / 2)))

    // A bare LIMIT (no ORDER BY) stops pulling generator rows early, so the streaming iterator is
    // abandoned before exhaustion -- this exercises the task-completion-listener parser cleanup.
    // Elements are in document order, so the first three ids are 1, 2, 3.
    checkAnswer(
      sql(
        s"""
           |SELECT id FROM json_table('$json', '$$.items[*]' COLUMNS (id INT PATH '$$.id')) AS t
           |LIMIT 3
         """.stripMargin),
      Seq(Row(1), Row(2), Row(3)))
  }

  test("deep container path to an array row source") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"a":{"b":[{"id":1},{"id":2}]}}',
        |  '$.a.b[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row(1), Row(2)))
  }

  test("indexed container path then wildcard") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"a":[{"vals":[{"id":10},{"id":20}]}]}',
        |  '$.a[0].vals[*]'
        |  COLUMNS (id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row(10), Row(20)))
  }

  test("non-explode row path resolving to JSON null yields one row of nulls") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"x":null}',
        |  '$.x'
        |  COLUMNS (v STRING PATH '$', present BOOLEAN EXISTS PATH '$')
        |) AS t
      """.stripMargin)
    // The row source is a JSON null: one row; a value column is SQL NULL, EXISTS is true.
    checkAnswer(df, Seq(Row(null, true)))
  }

  test("[*] over a JSON null container yields no rows / errors per ON ERROR") {
    val json = """{"items":null}"""
    checkAnswer(
      sql(
        s"""
           |SELECT * FROM json_table('$json', '$$.items[*]' COLUMNS (id INT PATH '$$.id')) AS t
         """.stripMargin),
      Seq.empty)
    intercept[SparkException] {
      sql(
        s"""
           |SELECT * FROM json_table(
           |  '$json', '$$.items[*]' COLUMNS (id INT PATH '$$.id') ERROR ON ERROR
           |) AS t
         """.stripMargin).collect()
    }
  }

  test("implicit path for a column name containing a dot reads the literal key") {
    // `a.b` with no PATH must read the JSON key "a.b", not the nested path a -> b.
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"a.b":1, "a":{"b":2}}]}',
        |  '$.items[*]'
        |  COLUMNS (`a.b` INT)
        |) AS t
      """.stripMargin)
    checkAnswer(df, Seq(Row(1)))
  }

  test("column cast eval mode is captured at plan construction, not execution") {
    // Build the DataFrame with ANSI off (bad cast -> NULL); enabling ANSI afterwards must not
    // change the already-planned generator's behavior.
    val query =
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"v":"not_a_number"}]}',
        |  '$.items[*]'
        |  COLUMNS (v INT PATH '$.v')
        |) AS t
      """.stripMargin
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df = sql(query)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkAnswer(df, Seq(Row(null)))
      }
    }
  }

  test("FOR ORDINALITY column is non-nullable in the output schema") {
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"id":1}]}',
        |  '$.items[*]'
        |  COLUMNS (seq FOR ORDINALITY, id INT PATH '$.id')
        |) AS t
      """.stripMargin)
    assert(!df.schema("seq").nullable)
    assert(df.schema("id").nullable)
  }

  test("castability check uses the session ANSI mode") {
    // BOOLEAN -> TIMESTAMP is castable in non-ANSI mode but not in ANSI mode, so an
    // `EXISTS ... TIMESTAMP` column must be accepted under non-ANSI and rejected under ANSI,
    // matching the eval mode of the actual per-column Cast.
    val query =
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"a":1}]}',
        |  '$.items[*]'
        |  COLUMNS (hasA TIMESTAMP EXISTS PATH '$.a')
        |) AS t
      """.stripMargin
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // Accepted at analysis; BOOLEAN true casts to a timestamp value.
      assert(sql(query).schema("hasA").dataType == TimestampType)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e = intercept[AnalysisException](sql(query).collect())
      assert(e.getCondition == "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION")
    }
  }

  test("CHAR/VARCHAR column types are normalized to STRING") {
    // A raw CHAR/VARCHAR target has no runtime encoder; like a normal CAST, the declared type is
    // normalized to STRING so the column is produced and queried without error.
    val df = sql(
      """
        |SELECT * FROM json_table(
        |  '{"items":[{"c":"hi","v":"world"}]}',
        |  '$.items[*]'
        |  COLUMNS (c CHAR(5) PATH '$.c', v VARCHAR(10) PATH '$.v')
        |) AS t
      """.stripMargin)
    assert(df.schema("c").dataType == StringType)
    assert(df.schema("v").dataType == StringType)
    checkAnswer(df, Seq(Row("hi", "world")))
  }

  test("wide projection interleaving all column kinds keeps every column in its own slot") {
    // The per-row projection reads column kinds, paths, casts and trie slots from parallel arrays,
    // so a wide projection that interleaves the three kinds (and leaves some paths unmatched)
    // pins each column to its own slot.
    val n = 60
    val fields = (1 to n).map(i => s""""f$i":$i""").mkString(",")
    val columns = (1 to n).map { i =>
      i % 3 match {
        case 0 => s"ord$i FOR ORDINALITY"
        case 1 => s"val$i INT PATH '$$.f$i'"
        // Some EXISTS columns point at a key the item does not have, so both outcomes are covered.
        case _ if i % 4 == 0 => s"ex$i BOOLEAN EXISTS PATH '$$.f$i'"
        case _ => s"ex$i BOOLEAN EXISTS PATH '$$.missing$i'"
      }
    }.mkString(", ")
    val df = sql(
      s"""
         |SELECT * FROM json_table('{"items":[{$fields}]}', '$$.items[*]' COLUMNS ($columns)) AS t
       """.stripMargin)
    val expected = (1 to n).map { i =>
      i % 3 match {
        case 0 => 1L // single row, so ordinality is 1
        case 1 => i
        case _ => i % 4 == 0
      }
    }
    checkAnswer(df, Seq(Row.fromSeq(expected)))
  }

  test("array row source over many input rows streams without leaking parsers") {
    // Exercises the per-task (not per-row) parser cleanup: many input rows, each with a `[*]`
    // array row source, must all shred correctly.
    withTempView("docs") {
      spark.range(0, 200)
        .selectExpr("concat('{\"items\":[{\"v\":', id, '},{\"v\":', id, '}]}') AS j")
        .createOrReplaceTempView("docs")
      val df = sql(
        """
          |SELECT t.v
          |FROM docs d,
          |LATERAL json_table(d.j, '$.items[*]' COLUMNS (v INT PATH '$.v')) AS t
        """.stripMargin)
      // Two rows per input document.
      assert(df.count() == 400)
    }
  }
}
