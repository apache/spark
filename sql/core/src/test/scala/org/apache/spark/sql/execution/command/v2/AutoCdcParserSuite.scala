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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.catalyst.analysis.{
  AnalysisTest, UnresolvedAttribute, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{
  AutoCdcIntoCommand,
  CreateFlowCommand,
  CreateStreamingTableAutoCdc
}
import org.apache.spark.sql.execution.SparkSqlParser

/**
 * Parser tests for AUTO CDC syntax.
 *
 * Covers two supported forms:
 *   1. CREATE FLOW <name> [COMMENT ...] AS AUTO CDC INTO <target> ...
 *   2. CREATE STREAMING TABLE <name> FLOW AUTO CDC ...
 *
 * Snapshot CDC, SCD Type 2, IGNORE NULL UPDATES, and APPLY AS TRUNCATE WHEN are not
 * supported and should fail to parse. The standalone AUTO CDC INTO form (without CREATE FLOW
 * or CREATE STREAMING TABLE) is also not supported.
 */
class AutoCdcParserSuite extends CommandSuiteBase with AnalysisTest {
  protected lazy val parser = new SparkSqlParser()

  // ---------------------------------------------------------------------------
  // CREATE FLOW ... AS AUTO CDC INTO
  // ---------------------------------------------------------------------------

  test("CREATE FLOW AS AUTO CDC INTO - minimal form") {
    val plan = parser.parsePlan(
      """CREATE FLOW myflow AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (key1, key2)
        |SEQUENCE BY timestamp""".stripMargin)

    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("myflow"))
    assert(cmd.comment.isEmpty)

    val cdc = cmd.flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.targetTable.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("target"))
    val source = cdc.source.asInstanceOf[UnresolvedRelation]
    assert(source.multipartIdentifier == Seq("source"))
    assert(source.isStreaming)
    assert(cdc.keys.map(_.name) == Seq("key1", "key2"))
    assert(cdc.deleteCondition.isEmpty)
    assert(cdc.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cdc.includeColumns.isEmpty)
    assert(cdc.excludeColumns.isEmpty)
  }

  test("CREATE FLOW AS AUTO CDC INTO - multipart source name") {
    val plan = parser.parsePlan(
      """CREATE FLOW myflow AS AUTO CDC INTO target
        |FROM STREAM(mycat.myschema.source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    val source = cdc.source.asInstanceOf[UnresolvedRelation]
    assert(source.multipartIdentifier == Seq("mycat", "myschema", "source"))
    assert(source.isStreaming)
  }

  test("CREATE FLOW AS AUTO CDC INTO - with COMMENT") {
    val plan = parser.parsePlan(
      """CREATE FLOW myflow COMMENT 'my comment' AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.comment == Some("my comment"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - multipart flow name") {
    val plan = parser.parsePlan(
      """CREATE FLOW mycat.myschema.myflow AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts ==
      Seq("mycat", "myschema", "myflow"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - two-part target table name") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO myschema.mytable
        |FROM STREAM(source)
        |KEYS (k)
        |SEQUENCE BY ts""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.targetTable.asInstanceOf[UnresolvedIdentifier].nameParts ==
      Seq("myschema", "mytable"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - three-part target table name") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO mycat.myschema.mytable
        |FROM STREAM(source)
        |KEYS (k)
        |SEQUENCE BY ts""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.targetTable.asInstanceOf[UnresolvedIdentifier].nameParts ==
      Seq("mycat", "myschema", "mytable"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - APPLY AS DELETE WHEN") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (id)
        |APPLY AS DELETE WHEN op = 'DELETE'
        |SEQUENCE BY ts""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.deleteCondition.isDefined)
    assert(cdc.deleteCondition.get.sql.contains("op"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - COLUMNS include list") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS (id, name, value)""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.includeColumns.get.map(_.name) == Seq("id", "name", "value"))
    assert(cdc.excludeColumns.isEmpty)
  }

  test("CREATE FLOW AS AUTO CDC INTO - COLUMNS * EXCEPT list") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS * EXCEPT (op, ts)""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.includeColumns.isEmpty)
    assert(cdc.excludeColumns.get.map(_.name) == Seq("op", "ts"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - all clauses combined") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM STREAM(source)
        |KEYS (key1, key2)
        |APPLY AS DELETE WHEN key3 = 3
        |SEQUENCE BY timestamp
        |COLUMNS (key1, key2, key3, timestamp)""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.keys.map(_.name) == Seq("key1", "key2"))
    assert(cdc.deleteCondition.isDefined)
    assert(cdc.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cdc.includeColumns.get.map(_.name) == Seq("key1", "key2", "key3", "timestamp"))
  }

  // ---------------------------------------------------------------------------
  // CREATE STREAMING TABLE ... FLOW AUTO CDC
  // ---------------------------------------------------------------------------

  test("CREATE STREAMING TABLE FLOW AUTO CDC - minimal form") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (key1, key2)
        |SEQUENCE BY timestamp""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("target"))
    assert(!cmd.ifNotExists)
    val source = cmd.source.asInstanceOf[UnresolvedRelation]
    assert(source.multipartIdentifier == Seq("source"))
    assert(source.isStreaming)
    assert(cmd.keys.map(_.name) == Seq("key1", "key2"))
    assert(cmd.deleteCondition.isEmpty)
    assert(cmd.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cmd.includeColumns.isEmpty)
    assert(cmd.excludeColumns.isEmpty)
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - multipart source name") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM STREAM(mycat.myschema.source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    val source = cmd.source.asInstanceOf[UnresolvedRelation]
    assert(source.multipartIdentifier == Seq("mycat", "myschema", "source"))
    assert(source.isStreaming)
  }

  test("CREATE STREAMING TABLE IF NOT EXISTS FLOW AUTO CDC") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE IF NOT EXISTS target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.ifNotExists)
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - multipart table name") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE myschema.mytable
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("myschema", "mytable"))
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - APPLY AS DELETE WHEN") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |APPLY AS DELETE WHEN op = 'DELETE'
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.deleteCondition.isDefined)
    assert(cmd.deleteCondition.get.sql.contains("op"))
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - COLUMNS include list") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS (id, name, value)""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.includeColumns.get.map(_.name) == Seq("id", "name", "value"))
    assert(cmd.excludeColumns.isEmpty)
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - COLUMNS * EXCEPT list") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS * EXCEPT (op, ts)""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.includeColumns.isEmpty)
    assert(cmd.excludeColumns.get.map(_.name) == Seq("op", "ts"))
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - all clauses combined") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (key1, key2)
        |APPLY AS DELETE WHEN key3 = 3
        |SEQUENCE BY timestamp
        |COLUMNS * EXCEPT (key4)""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.keys.map(_.name) == Seq("key1", "key2"))
    assert(cmd.deleteCondition.isDefined)
    assert(cmd.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cmd.excludeColumns.get.map(_.name) == Seq("key4"))
  }

  // ---------------------------------------------------------------------------
  // Error cases: missing required clause
  // ---------------------------------------------------------------------------

  test("CREATE FLOW AS AUTO CDC INTO - SEQUENCE BY is required") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "end of input", "hint" -> "")
    )
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - SEQUENCE BY is required") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE STREAMING TABLE target
            |FLOW AUTO CDC
            |FROM STREAM(source)
            |KEYS (id)""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "end of input", "hint" -> "")
    )
  }

  // ---------------------------------------------------------------------------
  // Error cases: wrong clause order
  // ---------------------------------------------------------------------------

  test("SEQUENCE BY before APPLY AS DELETE is not allowed") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |SEQUENCE BY ts
            |APPLY AS DELETE WHEN a = 1""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'APPLY'", "hint" -> "")
    )
  }

  test("COLUMNS before SEQUENCE BY is not allowed") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |COLUMNS a, b
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'COLUMNS'", "hint" -> "")
    )
  }

  // ---------------------------------------------------------------------------
  // Error cases: standalone form not supported
  // ---------------------------------------------------------------------------

  test("standalone AUTO CDC INTO is not supported") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'AUTO'", "hint" -> "")
    )
  }

  // ---------------------------------------------------------------------------
  // Error cases: source must be a STREAM(...)
  // ---------------------------------------------------------------------------

  test("CREATE FLOW AS AUTO CDC INTO - source without STREAM is not allowed") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM source
            |KEYS (id)
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'source'", "hint" -> "")
    )
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - source without STREAM is not allowed") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE STREAMING TABLE target
            |FLOW AUTO CDC
            |FROM source
            |KEYS (id)
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'source'", "hint" -> "")
    )
  }

  // ---------------------------------------------------------------------------
  // Error cases: KEYS and COLUMNS only accept simple identifiers
  // ---------------------------------------------------------------------------

  test("KEYS does not accept multipart identifiers") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (a.id)
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'.'", "hint" -> "")
    )
  }

  test("COLUMNS include list does not accept multipart identifiers") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |SEQUENCE BY ts
            |COLUMNS (a.name)""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'.'", "hint" -> "")
    )
  }

  test("COLUMNS * EXCEPT list does not accept multipart identifiers") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |SEQUENCE BY ts
            |COLUMNS * EXCEPT (a.op)""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'.'", "hint" -> "")
    )
  }

  // ---------------------------------------------------------------------------
  // Error cases: unsupported dataset types and table features
  // ---------------------------------------------------------------------------

  test("AUTO CDC is not supported for MATERIALIZED VIEW") {
    val sql =
      """CREATE MATERIALIZED VIEW target
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin
    checkError(
      intercept[ParseException] { parser.parsePlan(sql) },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map(
        "message" -> "AUTO CDC is only supported for STREAMING TABLE, not MATERIALIZED VIEW."),
      queryContext = Array(ExpectedContext(sql, 0, sql.length - 1))
    )
  }

  test("column constraints are not supported for AUTO CDC streaming table") {
    val sql =
      """CREATE STREAMING TABLE target (id INT PRIMARY KEY, name STRING)
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin
    checkError(
      intercept[ParseException] { parser.parsePlan(sql) },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Pipeline datasets do not currently support column constraints. " +
          "Please remove any CHECK, UNIQUE, PK, and FK constraints " +
          "specified on the pipeline dataset.")),
      queryContext = Array(ExpectedContext(sql, 0, sql.length - 1))
    )
  }

  test("bucketing is not supported for AUTO CDC streaming table") {
    val sql =
      """CREATE STREAMING TABLE target
        |CLUSTERED BY (id) INTO 4 BUCKETS
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin
    checkError(
      intercept[ParseException] { parser.parsePlan(sql) },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Bucketing is not supported for CREATE STREAMING TABLE statements. " +
          "Please remove any bucket spec specified in the statement.")),
      queryContext = Array(ExpectedContext(sql, 0, sql.length - 1))
    )
  }

  test("options are not supported for AUTO CDC streaming table") {
    val sql =
      """CREATE STREAMING TABLE target
        |OPTIONS (key = 'value')
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin
    checkError(
      intercept[ParseException] { parser.parsePlan(sql) },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Options are not supported for CREATE STREAMING TABLE statements. " +
          "Please remove any OPTIONS lists specified in the statement.")),
      queryContext = Array(ExpectedContext(sql, 0, sql.length - 1))
    )
  }

  test("serde is not supported for AUTO CDC streaming table") {
    val sql =
      """CREATE STREAMING TABLE target
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin
    checkError(
      intercept[ParseException] { parser.parsePlan(sql) },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Hive SerDe format options are not supported for " +
          "CREATE STREAMING TABLE statements.")),
      queryContext = Array(ExpectedContext(sql, 0, sql.length - 1))
    )
  }

  test("location is not supported for AUTO CDC streaming table") {
    val sql =
      """CREATE STREAMING TABLE target
        |LOCATION '/tmp/data'
        |FLOW AUTO CDC
        |FROM STREAM(source)
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin
    checkError(
      intercept[ParseException] { parser.parsePlan(sql) },
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Specifying location is not supported for CREATE STREAMING TABLE statements. " +
          "The storage location for a pipeline dataset is managed by the pipeline itself.")),
      queryContext = Array(ExpectedContext(sql, 0, sql.length - 1))
    )
  }

  // ---------------------------------------------------------------------------
  // Error cases: deprecated / unsupported syntax
  // ---------------------------------------------------------------------------

  test("APPLY AS TRUNCATE WHEN is not supported") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |APPLY AS TRUNCATE WHEN op = 'TRUNCATE'
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'TRUNCATE'", "hint" -> "")
    )
  }

  test("IGNORE NULL UPDATES is not supported") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |IGNORE NULL UPDATES
            |SEQUENCE BY ts""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'IGNORE'", "hint" -> "")
    )
  }

  test("STORED AS SCD TYPE 2 is not supported") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |SEQUENCE BY ts
            |STORED AS SCD TYPE 2""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'STORED'", "hint" -> "")
    )
  }

  test("TRACK HISTORY ON is not supported") {
    checkError(
      intercept[ParseException] {
        parser.parsePlan(
          """CREATE FLOW f AS AUTO CDC INTO target
            |FROM STREAM(source)
            |KEYS (id)
            |SEQUENCE BY ts
            |TRACK HISTORY ON value1, value2""".stripMargin)
      },
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "'TRACK'", "hint" -> "")
    )
  }
}
