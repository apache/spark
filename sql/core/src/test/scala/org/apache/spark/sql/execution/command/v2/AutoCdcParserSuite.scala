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
  AnalysisTest, UnresolvedAttribute,
  UnresolvedIdentifier, UnresolvedRelation}
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
        |FROM source
        |KEYS (key1, key2)
        |SEQUENCE BY timestamp""".stripMargin)

    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("myflow"))
    assert(cmd.comment.isEmpty)

    val cdc = cmd.flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.targetTable.table == "target")
    assert(cdc.sourceTable.isInstanceOf[UnresolvedRelation])
    assert(cdc.sourceTable.asInstanceOf[UnresolvedRelation].isStreaming)
    assert(cdc.keys.map(_.name) == Seq("key1", "key2"))
    assert(cdc.deleteCondition.isEmpty)
    assert(cdc.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cdc.specifiedCols.isEmpty)
    assert(cdc.exceptCols.isEmpty)
  }

  test("CREATE FLOW AS AUTO CDC INTO - with COMMENT") {
    val plan = parser.parsePlan(
      """CREATE FLOW myflow COMMENT 'my comment' AS AUTO CDC INTO target
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.comment == Some("my comment"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - multipart flow name") {
    val plan = parser.parsePlan(
      """CREATE FLOW mycat.myschema.myflow AS AUTO CDC INTO target
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts ==
      Seq("mycat", "myschema", "myflow"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - two-part target table name") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO myschema.mytable
        |FROM source
        |KEYS (k)
        |SEQUENCE BY ts""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.targetTable.database == Some("myschema"))
    assert(cdc.targetTable.table == "mytable")
  }

  test("CREATE FLOW AS AUTO CDC INTO - APPLY AS DELETE WHEN") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM source
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
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS id, name, value""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.specifiedCols.map(_.name) == Seq("id", "name", "value"))
    assert(cdc.exceptCols.isEmpty)
  }

  test("CREATE FLOW AS AUTO CDC INTO - COLUMNS * EXCEPT list") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS * EXCEPT (op, ts)""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.specifiedCols.isEmpty)
    assert(cdc.exceptCols.map(_.name) == Seq("op", "ts"))
  }

  test("CREATE FLOW AS AUTO CDC INTO - all clauses combined") {
    val plan = parser.parsePlan(
      """CREATE FLOW f AS AUTO CDC INTO target
        |FROM source
        |KEYS (key1, key2)
        |APPLY AS DELETE WHEN key3 = 3
        |SEQUENCE BY timestamp
        |COLUMNS key1, key2, key3, timestamp""".stripMargin)

    val cdc = plan.asInstanceOf[CreateFlowCommand].flowOperation.asInstanceOf[AutoCdcIntoCommand]
    assert(cdc.keys.map(_.name) == Seq("key1", "key2"))
    assert(cdc.deleteCondition.isDefined)
    assert(cdc.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cdc.specifiedCols.map(_.name) == Seq("key1", "key2", "key3", "timestamp"))
  }

  // ---------------------------------------------------------------------------
  // CREATE STREAMING TABLE ... FLOW AUTO CDC
  // ---------------------------------------------------------------------------

  test("CREATE STREAMING TABLE FLOW AUTO CDC - minimal form") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM source
        |KEYS (key1, key2)
        |SEQUENCE BY timestamp""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("target"))
    assert(!cmd.ifNotExists)
    assert(cmd.sourceTable.isInstanceOf[UnresolvedRelation])
    assert(cmd.sourceTable.asInstanceOf[UnresolvedRelation].isStreaming)
    assert(cmd.keys.map(_.name) == Seq("key1", "key2"))
    assert(cmd.deleteCondition.isEmpty)
    assert(cmd.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cmd.specifiedCols.isEmpty)
    assert(cmd.exceptCols.isEmpty)
  }

  test("CREATE STREAMING TABLE IF NOT EXISTS FLOW AUTO CDC") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE IF NOT EXISTS target
        |FLOW AUTO CDC
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.ifNotExists)
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - multipart table name") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE myschema.mytable
        |FLOW AUTO CDC
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == Seq("myschema", "mytable"))
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - APPLY AS DELETE WHEN") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM source
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
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS id, name, value""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.specifiedCols.map(_.name) == Seq("id", "name", "value"))
    assert(cmd.exceptCols.isEmpty)
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - COLUMNS * EXCEPT list") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM source
        |KEYS (id)
        |SEQUENCE BY ts
        |COLUMNS * EXCEPT (op, ts)""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.specifiedCols.isEmpty)
    assert(cmd.exceptCols.map(_.name) == Seq("op", "ts"))
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - all clauses combined") {
    val plan = parser.parsePlan(
      """CREATE STREAMING TABLE target
        |FLOW AUTO CDC
        |FROM source
        |KEYS (key1, key2)
        |APPLY AS DELETE WHEN key3 = 3
        |SEQUENCE BY timestamp
        |COLUMNS * EXCEPT (key4)""".stripMargin)

    val cmd = plan.asInstanceOf[CreateStreamingTableAutoCdc]
    assert(cmd.keys.map(_.name) == Seq("key1", "key2"))
    assert(cmd.deleteCondition.isDefined)
    assert(cmd.sequenceByExpr == UnresolvedAttribute("timestamp"))
    assert(cmd.exceptCols.map(_.name) == Seq("key4"))
  }

  // ---------------------------------------------------------------------------
  // Error cases: missing required clause
  // ---------------------------------------------------------------------------

  test("CREATE FLOW AS AUTO CDC INTO - SEQUENCE BY is required") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  test("CREATE STREAMING TABLE FLOW AUTO CDC - SEQUENCE BY is required") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE target
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  // ---------------------------------------------------------------------------
  // Error cases: wrong clause order
  // ---------------------------------------------------------------------------

  test("SEQUENCE BY before APPLY AS DELETE is not allowed") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts
          |APPLY AS DELETE WHEN a = 1""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  test("COLUMNS before SEQUENCE BY is not allowed") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |COLUMNS a, b
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  // ---------------------------------------------------------------------------
  // Error cases: standalone form not supported
  // ---------------------------------------------------------------------------

  test("standalone AUTO CDC INTO is not supported") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  // ---------------------------------------------------------------------------
  // Error cases: unsupported dataset types and table features
  // ---------------------------------------------------------------------------

  test("AUTO CDC is not supported for MATERIALIZED VIEW") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE MATERIALIZED VIEW target
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getMessage.contains("AUTO CDC is only supported for STREAMING TABLE"))
  }

  test("column constraints are not supported for AUTO CDC streaming table") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE target (id INT PRIMARY KEY, name STRING)
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getMessage.contains("column constraints"))
  }

  test("bucketing is not supported for AUTO CDC streaming table") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE target
          |CLUSTERED BY (id) INTO 4 BUCKETS
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getMessage.contains("Bucketing is not supported"))
  }

  test("options are not supported for AUTO CDC streaming table") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE target
          |OPTIONS (key = 'value')
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getMessage.contains("Options are not supported"))
  }

  test("serde is not supported for AUTO CDC streaming table") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE target
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getMessage.contains("SerDe"))
  }

  test("location is not supported for AUTO CDC streaming table") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE target
          |LOCATION '/tmp/data'
          |FLOW AUTO CDC
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getMessage.contains("location is not supported"))
  }

  // ---------------------------------------------------------------------------
  // Error cases: deprecated / unsupported syntax
  // ---------------------------------------------------------------------------

  test("APPLY AS TRUNCATE WHEN is not supported") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |APPLY AS TRUNCATE WHEN op = 'TRUNCATE'
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  test("IGNORE NULL UPDATES is not supported") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |IGNORE NULL UPDATES
          |SEQUENCE BY ts""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  test("STORED AS SCD TYPE 2 is not supported") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts
          |STORED AS SCD TYPE 2""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }

  test("TRACK HISTORY ON is not supported") {
    val e = intercept[ParseException] {
      parser.parsePlan(
        """CREATE FLOW f AS AUTO CDC INTO target
          |FROM source
          |KEYS (id)
          |SEQUENCE BY ts
          |TRACK HISTORY ON value1, value2""".stripMargin)
    }
    assert(e.getCondition == "PARSE_SYNTAX_ERROR")
  }
}
