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

package org.apache.spark.sql.catalyst.expressions

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{
  BoundProcedure,
  ProcedureParameter,
  UnboundProcedure
}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

class ParseSqlAnalyzeSuite extends QueryTest with SharedSparkSession {

  private def evalJson(sql: String, mode: String = "ANALYZE"): JValue = {
    val result = spark.sql(s"SELECT parse_sql($sql, '$mode')").head().getString(0)
    parse(result)
  }

  private def evalStatement(sql: String, mode: String = "ANALYZE"): JObject = {
    evalJson("'" + sql.replace("'", "''") + "'", mode) match {
      case JArray(List(value: JObject)) => value
      case other => fail(s"expected one statement object, got: $other")
    }
  }

  private def jsonType(name: String): JObject = JObject("name" -> JString(name))
  private val jsonInt: JObject = jsonType("int")
  private val jsonBigint: JObject = jsonType("bigint")
  private val jsonBoolean: JObject = jsonType("boolean")
  private val jsonString: JObject = JObject(
    "name" -> JString("string"),
    "collation" -> JString("UTF8_BINARY"))

  test("ANALYZE types selected columns and PARSE does not") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val analyzed = evalStatement("SELECT 1 AS a", "ANALYZE")
      assert(analyzed \ "parse_success" === JBool(true))
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt))))

      val parsed = evalStatement("SELECT 1 AS a", "PARSE")
      assert(parsed \ "analyze_success" === JNothing)
      assert(parsed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a")))))))
    }
  }

  test("ANALYZE fully qualifies tables and builtin routines") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_analyze_t") {
        sql("CREATE TABLE parse_sql_analyze_t (a INT, b STRING) USING parquet")
        val j = evalStatement(
          "SELECT count(a) AS c FROM parse_sql_analyze_t", "analyze")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(JArray(List(
          JString("spark_catalog"), JString("default"), JString("parse_sql_analyze_t"))))))
        assert(j \ "function_references" === JArray(List(JArray(List(
          JString("system"), JString("builtin"), JString("count"))))))
        assert(j \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("c"))), "type" -> jsonBigint))))
      }
    }
  }

  test("ANALYZE qualifies local temp views as system.session") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_analyze_v") {
        sql("CREATE OR REPLACE TEMP VIEW parse_sql_analyze_v AS SELECT 1 AS a, 'x' AS b")
        val j = evalStatement("SELECT a, b FROM parse_sql_analyze_v")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(JArray(List(
          JString("system"), JString("session"), JString("parse_sql_analyze_v"))))))
        assert(j \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt),
          JObject("name" -> JArray(List(JString("b"))), "type" -> jsonString))))
      }
    }
  }

  test("ANALYZE keeps parse metadata when analysis fails") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val j = evalStatement("SELECT a FROM missing_parse_sql_analyze_t")
      assert(j \ "parse_success" === JBool(true))
      assert(j \ "analyze_success" === JBool(false))
      assert(j \ "statement_identifier" === JString("SELECT"))
      assert(j \ "source_table_references" === JArray(List(JArray(List(
        JString("spark_catalog"),
        JString("default"),
        JString("missing_parse_sql_analyze_t"))))))
      val errorClass = (j \ "error" \ "errorClass").asInstanceOf[JString].s
      assert(errorClass == "TABLE_OR_VIEW_NOT_FOUND" ||
        errorClass.contains("TABLE_OR_VIEW_NOT_FOUND"))
    }
  }

  test("ANALYZE inherits invoker schema and can override current_schema") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_ctx_a")
      sql("CREATE DATABASE parse_sql_ctx_b")
      try {
        sql("CREATE TABLE parse_sql_ctx_a.t (a INT) USING parquet")
        sql("USE parse_sql_ctx_b")
        try {
          val missing = evalStatement("SELECT a FROM t")
          assert(missing \ "analyze_success" === JBool(false))
          assert(missing \ "source_table_references" === JArray(List(JArray(List(
            JString("spark_catalog"), JString("parse_sql_ctx_b"), JString("t"))))))

          val json = spark.sql(
            """SELECT parse_sql(
              |  'SELECT a FROM t',
              |  'ANALYZE',
              |  current_schema => 'parse_sql_ctx_a')""".stripMargin).head().getString(0)
          val analyzed = parse(json) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(analyzed \ "analyze_success" === JBool(true))
          assert(analyzed \ "source_table_references" === JArray(List(JArray(List(
            JString("spark_catalog"), JString("parse_sql_ctx_a"), JString("t"))))))
          assert(spark.sql("SELECT current_schema()").head().getString(0) ===
            "parse_sql_ctx_b")
        } finally {
          sql("USE default")
        }
      } finally {
        sql("DROP TABLE IF EXISTS parse_sql_ctx_a.t")
        sql("DROP DATABASE IF EXISTS parse_sql_ctx_a CASCADE")
        sql("DROP DATABASE IF EXISTS parse_sql_ctx_b CASCADE")
      }
    }
  }

  test("ANALYZE named mode and current_catalog inherit from invoker") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT 1 AS a',
          |  mode => 'ANALYZE',
          |  current_catalog => current_catalog())""".stripMargin).head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt))))
    }
  }

  test("ANALYZE current_path override is restored on the invoker") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.PATH_ENABLED.key -> "true") {
      sql("SET PATH = spark_catalog.default, system.builtin")
      try {
        val before = spark.sql("SELECT current_path()").head().getString(0)
        val json = spark.sql(
          """SELECT parse_sql(
            |  'SELECT 1 AS a',
            |  'ANALYZE',
            |  current_path => 'system.builtin, spark_catalog.default')""".stripMargin)
          .head().getString(0)
        val analyzed = parse(json) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(analyzed \ "analyze_success" === JBool(true))
        assert(spark.sql("SELECT current_path()").head().getString(0) === before)
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("ANALYZE reports invalid current_schema as JSON instead of failing the query") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        "SELECT parse_sql('SELECT 1 AS a', 'ANALYZE', current_schema => '!!!')")
        .head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "parse_success" === JBool(true))
      assert(analyzed \ "analyze_success" === JBool(false))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a")))))))
      assert((analyzed \ "error" \ "errorClass").asInstanceOf[JString].s ===
        "INVALID_PARAMETER_VALUE.PARSE_SQL_CONTEXT")
    }
  }

  test("ANALYZE resolves injected session variables and restores the invoker") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("DECLARE VARIABLE parse_sql_existing_v INT DEFAULT 1")
      try {
        val json = spark.sql(
          """SELECT parse_sql(
            |  'SELECT system.session.v AS x',
            |  'ANALYZE',
            |  variables => array(named_struct('name', 'system.session.v', 'type', 'INT')))
            |""".stripMargin).head().getString(0)
        val analyzed = parse(json) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(analyzed \ "analyze_success" === JBool(true))
        assert(analyzed \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("x"))), "type" -> jsonInt))))
        assert(spark.sql("SELECT parse_sql_existing_v").head().getInt(0) === 1)
      } finally {
        sql("DROP TEMPORARY VARIABLE parse_sql_existing_v")
      }
    }
  }

  test("ANALYZE resolves injected procedure variables") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      Seq("false", "true").foreach { singlePass =>
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
          val json = spark.sql(
            """SELECT parse_sql(
              |  'SELECT proc.parm1 AS x',
              |  'ANALYZE',
              |  variables => array(named_struct('name', 'proc.parm1', 'type', 'STRING')))
              |""".stripMargin).head().getString(0)
          val analyzed = parse(json) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(analyzed \ "analyze_success" === JBool(true))
          assert(analyzed \ "select_list" === JArray(List(
            JObject("name" -> JArray(List(JString("x"))), "type" -> jsonString))))
        }
      }
    }
  }

  test("ANALYZE supplied session variables shadow persisted ones without mutating them") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("DECLARE VARIABLE parse_sql_shadow_v INT DEFAULT 1")
      try {
        Seq("false", "true").foreach { singlePass =>
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
            val json = spark.sql(
              """SELECT parse_sql(
                |  'SELECT parse_sql_shadow_v AS x',
                |  'ANALYZE',
                |  variables => array(named_struct(
                |    'name', 'parse_sql_shadow_v',
                |    'type', 'STRING')))
                |""".stripMargin).head().getString(0)
            val analyzed = parse(json) match {
              case JArray(List(value: JObject)) => value
              case other => fail(s"expected one statement object, got: $other")
            }
            assert(analyzed \ "analyze_success" === JBool(true))
            assert(analyzed \ "select_list" === JArray(List(
              JObject("name" -> JArray(List(JString("x"))), "type" -> jsonString))))
            assert(spark.sql("SELECT parse_sql_shadow_v").head().getInt(0) === 1)
          }
        }
      } finally {
        sql("DROP TEMPORARY VARIABLE parse_sql_shadow_v")
      }
    }
  }

  test("ANALYZE binds supplied local variables by identity") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT parm1 AS x',
          |  'ANALYZE',
          |  variables => array(named_struct('name', 'proc.parm1', 'type', 'STRING')))
          |""".stripMargin).head().getString(0)
      val miss = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(miss \ "analyze_success" === JBool(false))
    }
  }

  test("ANALYZE unqualified session variables follow the SQL path") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.PATH_ENABLED.key -> "true") {
      sql("SET PATH = spark_catalog.default, system.builtin")
      try {
        val missedJson = spark.sql(
          """SELECT parse_sql(
            |  'SELECT v AS x',
            |  'ANALYZE',
            |  current_path => 'spark_catalog.default, system.builtin',
            |  variables => array(named_struct(
            |    'name', 'system.session.v',
            |    'type', 'INT')))
            |""".stripMargin).head().getString(0)
        val miss = parse(missedJson) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(miss \ "analyze_success" === JBool(false))

        val hitJson = spark.sql(
          """SELECT parse_sql(
            |  'SELECT v AS x',
            |  'ANALYZE',
            |  current_path => 'system.session, spark_catalog.default, system.builtin',
            |  variables => array(named_struct(
            |    'name', 'system.session.v',
            |    'type', 'INT')))
            |""".stripMargin).head().getString(0)
        val hit = parse(hitJson) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(hit \ "analyze_success" === JBool(true))
        assert(hit \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("x"))), "type" -> jsonInt))))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("ANALYZE resolves injected relations from a DDL column list") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT a, b FROM t',
          |  'ANALYZE',
          |  relations => array(named_struct('name', 't', 'schema', 'a INT, b STRING')))
          |""".stripMargin).head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt),
        JObject("name" -> JArray(List(JString("b"))), "type" -> jsonString))))
      assert(analyzed \ "source_table_references" === JArray(List(JArray(List(
        JString("spark_catalog"), JString("default"), JString("t"))))))
    }
  }

  test("ANALYZE one-part overlays eclipse persistent tables; PATH still prefers temp views") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE TABLE parse_sql_overlay_t (a BIGINT) USING parquet")
      try {
        Seq("false", "true").foreach { singlePass =>
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
          val supplied = spark.sql(
            """SELECT parse_sql(
              |  'SELECT a FROM parse_sql_overlay_t',
              |  'ANALYZE',
              |  relations => array(named_struct(
              |    'name', 'parse_sql_overlay_t',
              |    'schema', 'a STRING')))
              |""".stripMargin).head().getString(0)
          val suppliedJson = parse(supplied) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(suppliedJson \ "analyze_success" === JBool(true))
          assert(suppliedJson \ "select_list" === JArray(List(
            JObject("name" -> JArray(List(JString("a"))), "type" -> jsonString))))
          }
          sql("CREATE OR REPLACE TEMP VIEW parse_sql_overlay_t AS SELECT true AS a")
          try {
            withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
            val temp = spark.sql(
              """SELECT parse_sql(
                |  'SELECT a FROM parse_sql_overlay_t',
                |  'ANALYZE',
                |  relations => array(named_struct(
                |    'name', 'parse_sql_overlay_t',
                |    'schema', 'a STRING')))
                |""".stripMargin).head().getString(0)
            val tempJson = parse(temp) match {
              case JArray(List(value: JObject)) => value
              case other => fail(s"expected one statement object, got: $other")
            }
            assert(tempJson \ "analyze_success" === JBool(true))
            assert(tempJson \ "select_list" === JArray(List(
              JObject("name" -> JArray(List(JString("a"))), "type" -> jsonBoolean))))
            }
          } finally {
            sql("DROP VIEW parse_sql_overlay_t")
          }
        }
      } finally {
        sql("DROP TABLE parse_sql_overlay_t")
      }
    }
  }

  test("ANALYZE system.session overlay eclipses a temp view as source and INSERT target") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_v") {
        sql("CREATE OR REPLACE TEMP VIEW parse_sql_v AS SELECT 1 AS a")
        Seq("false", "true").foreach { singlePass =>
          withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
            val source = spark.sql(
              """SELECT parse_sql(
                |  'SELECT b FROM parse_sql_v',
                |  'ANALYZE',
                |  relations => array(named_struct(
                |    'name', 'system.session.parse_sql_v',
                |    'schema', 'b INT')))
                |""".stripMargin).head().getString(0)
            val sourceJson = parse(source) match {
              case JArray(List(value: JObject)) => value
              case other => fail(s"expected one statement object, got: $other")
            }
            assert(sourceJson \ "analyze_success" === JBool(true))
            assert(sourceJson \ "select_list" === JArray(List(
              JObject("name" -> JArray(List(JString("b"))), "type" -> jsonInt))))
            assert(sourceJson \ "source_table_references" === JArray(List(JArray(List(
              JString("system"), JString("session"), JString("parse_sql_v"))))))
          }
        }
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false") {
          val insert = spark.sql(
            """SELECT parse_sql(
              |  'INSERT INTO parse_sql_v VALUES (2)',
              |  'ANALYZE',
              |  relations => array(named_struct(
              |    'name', 'system.session.parse_sql_v',
              |    'schema', 'b INT')))
              |""".stripMargin).head().getString(0)
          val insertJson = parse(insert) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(insertJson \ "analyze_success" === JBool(true), insert)
          assert(insertJson \ "target_table_references" === JArray(List(JArray(List(
            JString("system"), JString("session"), JString("parse_sql_v"))))))
        }
      }
    }
  }

  test("ANALYZE binds qualified relation overlays by identity, not session temp names") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_rel_db")
      try {
        val qualified = spark.sql(
          """SELECT parse_sql(
            |  'SELECT a FROM spark_catalog.parse_sql_rel_db.t',
            |  'ANALYZE',
            |  relations => array(named_struct(
            |    'name', 'spark_catalog.parse_sql_rel_db.t',
            |    'schema', 'a INT')))
            |""".stripMargin).head().getString(0)
        val hit = parse(qualified) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(hit \ "analyze_success" === JBool(true))
        assert(hit \ "source_table_references" === JArray(List(JArray(List(
          JString("spark_catalog"), JString("parse_sql_rel_db"), JString("t"))))))
        assert(hit \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt))))

        // The overlay is a different schema; unqualified t in default does not bind.
        val missedJson = spark.sql(
          """SELECT parse_sql(
            |  'SELECT a FROM t',
            |  'ANALYZE',
            |  relations => array(named_struct(
            |    'name', 'spark_catalog.parse_sql_rel_db.t',
            |    'schema', 'a INT')))
            |""".stripMargin).head().getString(0)
        val miss = parse(missedJson) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(miss \ "analyze_success" === JBool(false))
        assert(miss \ "source_table_references" === JArray(List(JArray(List(
          JString("spark_catalog"), JString("default"), JString("t"))))))

        val viaSchema = spark.sql(
          """SELECT parse_sql(
            |  'SELECT a FROM t',
            |  'ANALYZE',
            |  current_schema => 'parse_sql_rel_db',
            |  relations => array(named_struct(
            |    'name', 'spark_catalog.parse_sql_rel_db.t',
            |    'schema', 'a INT')))
            |""".stripMargin).head().getString(0)
        val via = parse(viaSchema) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(via \ "analyze_success" === JBool(true))
        assert(via \ "source_table_references" === JArray(List(JArray(List(
          JString("spark_catalog"), JString("parse_sql_rel_db"), JString("t"))))))

        withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
          sql("SET PATH = spark_catalog.parse_sql_rel_db, system.builtin")
          try {
            val viaPath = spark.sql(
              """SELECT parse_sql(
                |  'SELECT a FROM t',
                |  'ANALYZE',
                |  relations => array(named_struct(
                |    'name', 'spark_catalog.parse_sql_rel_db.t',
                |    'schema', 'a INT')))
                |""".stripMargin).head().getString(0)
            val path = parse(viaPath) match {
              case JArray(List(value: JObject)) => value
              case other => fail(s"expected one statement object, got: $other")
            }
            assert(path \ "analyze_success" === JBool(true))
            assert(path \ "select_list" === JArray(List(
              JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt))))
          } finally {
            sql("SET PATH = DEFAULT_PATH")
          }
        }
      } finally {
        sql("DROP DATABASE parse_sql_rel_db")
      }
    }
  }

  test("ANALYZE reports invalid variable types as JSON") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT 1 AS a',
          |  'ANALYZE',
          |  variables => array(named_struct('name', 'v', 'type', '!!!')))
          |""".stripMargin).head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "parse_success" === JBool(true))
      assert(analyzed \ "analyze_success" === JBool(false))
      assert((analyzed \ "error" \ "errorClass").asInstanceOf[JString].s ===
        "INVALID_PARAMETER_VALUE.PARSE_SQL_CONTEXT")
    }
  }

  test("ANALYZE binds supplied positional and named parameter markers") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      Seq("false", "true").foreach { singlePass =>
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
          val pos = spark.sql(
            """SELECT parse_sql(
              |  'SELECT abs(?) AS x',
              |  'ANALYZE',
              |  parameter_markers => array(named_struct('name', '?', 'type', 'INT')))
              |""".stripMargin).head().getString(0)
          val posJson = parse(pos) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(posJson \ "analyze_success" === JBool(true))
          assert(posJson \ "select_list" === JArray(List(
            JObject("name" -> JArray(List(JString("x"))), "type" -> jsonInt))))
          assert(posJson \ "parameter_markers" \ "unnamed_count" === JInt(1))

          val named = spark.sql(
            """SELECT parse_sql(
              |  'SELECT :p AS x',
              |  'ANALYZE',
              |  parameter_markers => array(named_struct('name', 'p', 'type', 'STRING')))
              |""".stripMargin).head().getString(0)
          val namedJson = parse(named) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(namedJson \ "analyze_success" === JBool(true))
          assert(namedJson \ "select_list" === JArray(List(
            JObject("name" -> JArray(List(JString("x"))), "type" -> jsonString))))
          assert(namedJson \ "parameter_markers" \ "named" ===
            JArray(List(JString("p"))))
        }
      }
    }
  }

  test("ANALYZE without parameter marker types leaves the query unresolved") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql("SELECT parse_sql('SELECT abs(?) AS x', 'ANALYZE')")
        .head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "parse_success" === JBool(true))
      assert(analyzed \ "analyze_success" === JBool(false))
    }
  }

  test("ANALYZE reports invalid parameter marker types as JSON") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT 1 AS a',
          |  'ANALYZE',
          |  parameter_markers => array(named_struct('name', '?', 'type', '!!!')))
          |""".stripMargin).head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "analyze_success" === JBool(false))
      assert((analyzed \ "error" \ "errorClass").asInstanceOf[JString].s ===
        "INVALID_PARAMETER_VALUE.PARSE_SQL_CONTEXT")
    }
  }

  test("PARSE ignores injected parameter markers") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT ? AS x',
          |  'PARSE',
          |  parameter_markers => array(named_struct('name', '?', 'type', 'INT')))
          |""".stripMargin).head().getString(0)
      val parsed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(parsed \ "parse_success" === JBool(true))
      assert(parsed \ "analyze_success" === JNothing)
      assert(parsed \ "parameter_markers" \ "unnamed_count" === JInt(1))
    }
  }

  test("PARSE ignores injected relations") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT a FROM missing_parse_sql_rel',
          |  'PARSE',
          |  relations => array(named_struct('name', 'missing_parse_sql_rel', 'schema', 'a INT')))
          |""".stripMargin).head().getString(0)
      val parsed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(parsed \ "parse_success" === JBool(true))
      assert(parsed \ "analyze_success" === JNothing)
    }
  }

  private def evalBatch(sql: String): List[JObject] =
    evalJson("'" + sql.replace("'", "''") + "'") match {
      case JArray(values) => values.map(_.asInstanceOf[JObject])
      case other => fail(s"expected a JSON array, got: $other")
    }

  private def nested(script: JObject): List[JObject] = script \ "statements" match {
    case JArray(values) => values.map(_.asInstanceOf[JObject])
    case other => fail(s"expected nested statements, got: $other")
  }

  private def typedColumns(columns: (String, JObject)*): JArray = JArray(columns.map {
    case (name, dataType) => JObject("name" -> JArray(List(JString(name))), "type" -> dataType)
  }.toList)

  private def ref(parts: String*): JArray = JArray(parts.map(JString).toList)

  test("ANALYZE script types locals from DECLARE type or DEFAULT") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE x INT DEFAULT 5;
          |  DECLARE s DEFAULT 'a';
          |  DECLARE d DECIMAL(10, 2);
          |  DECLARE e DEFAULT x * 2L;
          |  SELECT x + 1 AS y, s AS t, d AS u, e AS v;
          |END""".stripMargin)
      assert(script \ "statement_identifier" === JString("BEGIN END"))
      assert(script \ "analyze_success" === JBool(true))
      val statements = nested(script)
      assert(statements.map(_ \ "analyze_success").distinct === List(JBool(true)))
      assert(statements.last \ "select_list" === typedColumns(
        "y" -> jsonInt,
        "t" -> jsonString,
        "u" -> JObject(
          "name" -> JString("decimal"), "precision" -> JInt(10), "scale" -> JInt(2)),
        "v" -> jsonBigint))
      assert(spark.sessionState.catalogManager.tempVariableManager.isEmpty)
    }
  }

  test("ANALYZE script resolves locals by scope and label") {
    Seq("false", "true").foreach { singlePass =>
      withSQLConf(
          SQLConf.PARSE_SQL_ENABLED.key -> "true",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
        checkScopesAndLabels()
      }
    }
  }

  private def checkScopesAndLabels(): Unit = {
    val script = evalStatement(
      """BEGIN
        |  DECLARE x INT DEFAULT 1;
        |  lbl: BEGIN
        |    DECLARE x STRING DEFAULT 'a';
        |    SELECT x AS inner_x, lbl.x AS lbl_x;
        |  END lbl;
        |  SELECT x AS outer_x;
        |  blk: BEGIN
        |    DECLARE z INT DEFAULT 1;
        |  END blk;
        |  SELECT z AS gone;
        |END""".stripMargin)
    val selects = nested(script).filter(_ \ "statement_identifier" == JString("SELECT"))
    assert(selects.head \ "select_list" === typedColumns(
      "inner_x" -> jsonString, "lbl_x" -> jsonString))
    assert(selects(1) \ "select_list" === typedColumns("outer_x" -> jsonInt))
    assert(selects(2) \ "analyze_success" === JBool(false))
    assert(selects(2) \ "error" \ "errorClass" ===
      JString("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION"))
    assert(script \ "analyze_success" === JBool(false))
  }

  test("ANALYZE script binds FOR loop columns and control-flow conditions") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE c INT DEFAULT 0;
          |  WHILE c < 3 DO
          |    SET c = c + 1;
          |  END WHILE;
          |  FOR r AS SELECT 1 AS id, 'n' AS name DO
          |    SELECT id AS i, r.name AS n, c AS k;
          |  END FOR;
          |  IF c = 3 THEN SELECT c AS done; END IF;
          |END""".stripMargin)
      assert(script \ "analyze_success" === JBool(true))
      val statements = nested(script)
      assert(statements.length === 7)
      assert(statements(1) \ "select_list" === typedColumns("condition" -> jsonBoolean))
      assert(statements(4) \ "select_list" === typedColumns(
        "i" -> jsonInt, "n" -> jsonString, "k" -> jsonInt))
    }
  }

  test("ANALYZE script analyzes handler bodies and simple CASE branches in scope") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE code STRING DEFAULT 'x';
          |  DECLARE EXIT HANDLER FOR SQLEXCEPTION SELECT code AS failed;
          |  CASE code
          |    WHEN 'x' THEN SELECT 1 AS one;
          |    ELSE SELECT code || '!' AS other;
          |  END CASE;
          |END""".stripMargin)
      assert(script \ "analyze_success" === JBool(true))
      val selected = nested(script).map(_ \ "select_list").collect {
        case JArray(List(column)) => column
      }
      assert(selected === List(
        "failed" -> jsonString,
        "condition" -> jsonBoolean,
        "one" -> jsonInt,
        "other" -> jsonString).map { case (name, dataType) =>
          JObject("name" -> JArray(List(JString(name))), "type" -> dataType)
        })
    }
  }

  test("ANALYZE script sees tables and views it creates without creating them") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val results = evalBatch(
        """BEGIN
          |  CREATE TABLE parse_sql_script_t (a INT, b STRING);
          |  CREATE TEMPORARY VIEW parse_sql_script_v AS
          |    SELECT a * 2L AS a2 FROM parse_sql_script_t;
          |  INSERT INTO parse_sql_script_t SELECT a2, 'x' FROM parse_sql_script_v;
          |  CREATE TABLE parse_sql_script_c AS
          |    SELECT b, a2 FROM parse_sql_script_t, parse_sql_script_v;
          |  SELECT * FROM parse_sql_script_c;
          |END;
          |SELECT b FROM parse_sql_script_t""".stripMargin)
      val script = results.head
      assert(script \ "analyze_success" === JBool(true))
      val statements = nested(script)
      assert(statements.map(_ \ "analyze_success").distinct === List(JBool(true)))
      assert(statements(4) \ "select_list" === typedColumns("b" -> jsonString, "a2" -> jsonBigint))
      val table = ref("spark_catalog", "default", "parse_sql_script_t")
      val view = ref("system", "session", "parse_sql_script_v")
      assert(statements(2) \ "target_table_references" === JArray(List(table)))
      assert(statements(2) \ "source_table_references" === JArray(List(view)))
      assert(script \ "source_table_references" === JArray(List(table, view,
        ref("spark_catalog", "default", "parse_sql_script_c"))))
      assert(results(1) \ "analyze_success" === JBool(true))
      assert(results(1) \ "select_list" === typedColumns("b" -> jsonString))
      assert(!spark.catalog.tableExists("parse_sql_script_t"))
      assert(!spark.catalog.tableExists("parse_sql_script_v"))
    }
  }

  test("ANALYZE batch carries DECLARE VARIABLE and DDL to later statements") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val results = evalBatch(
        """DECLARE VARIABLE parse_sql_batch_v BIGINT DEFAULT 1;
          |SELECT parse_sql_batch_v AS x;
          |CREATE TABLE parse_sql_batch_t (a INT);
          |SELECT a + parse_sql_batch_v AS y FROM parse_sql_batch_t;
          |DROP TABLE parse_sql_batch_t;
          |DROP TEMPORARY VARIABLE parse_sql_batch_v;
          |SELECT parse_sql_batch_v AS z;
          |SELECT a FROM parse_sql_batch_t""".stripMargin)
      assert(results(1) \ "select_list" === typedColumns("x" -> jsonBigint))
      assert(results(3) \ "select_list" === typedColumns("y" -> jsonBigint))
      assert(results(6) \ "analyze_success" === JBool(false))
      assert(results(7) \ "analyze_success" === JBool(false))
      assert(spark.sessionState.catalogManager.tempVariableManager.isEmpty)
      assert(!spark.catalog.tableExists("parse_sql_batch_t"))
    }
  }

  test("ANALYZE script resolves DECLARE CURSOR, OPEN, FETCH, and CLOSE") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE x INT;
          |  CREATE TABLE parse_sql_cur_t (id INT);
          |  lbl: BEGIN
          |    DECLARE c CURSOR FOR SELECT id FROM parse_sql_cur_t;
          |    OPEN c;
          |    FETCH c INTO x;
          |    FETCH lbl.c INTO x;
          |    CLOSE c;
          |  END lbl;
          |END""".stripMargin)
      assert(script \ "analyze_success" === JBool(true))
      val statements = nested(script)
      assert(statements.map(_ \ "analyze_success").distinct === List(JBool(true)))
      val declareCur = statements.find { s =>
        (s \ "source_table_references") != JNothing
      }.get
      assert(declareCur \ "source_table_references" === JArray(List(
        ref("spark_catalog", "default", "parse_sql_cur_t"))))
      assert(declareCur \ "select_list" === typedColumns("id" -> jsonInt))
      assert(!spark.catalog.tableExists("parse_sql_cur_t"))
    }
  }

  test("PARSE script DECLARE CURSOR lists the query tables") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "true") {
      val script = evalStatement(
        "BEGIN DECLARE c CURSOR FOR SELECT a FROM t; OPEN c; END", "PARSE")
      val declareCur = nested(script).head
      assert(declareCur \ "source_table_references" === JArray(List(JArray(List(JString("t"))))))
      assert(declareCur \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a")))))))
    }
  }

  test("ANALYZE script FETCH of an unknown cursor is CURSOR_NOT_FOUND") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE x INT;
          |  FETCH missing_cur INTO x;
          |END""".stripMargin)
      val fetch = nested(script).find(s => (s \ "error") != JNothing).get
      assert(fetch \ "analyze_success" === JBool(false))
      assert(fetch \ "error" \ "errorClass" === JString("CURSOR_NOT_FOUND"))
    }
  }

  /** Run a full `SELECT parse_sql(...)` and return its statement objects. */
  private def evalCall(query: String): List[JObject] =
    parse(spark.sql(query).head().getString(0)) match {
      case JArray(values) => values.map(_.asInstanceOf[JObject])
      case other => fail(s"expected a JSON array, got: $other")
    }

  private def errorClass(statement: JObject): JValue = statement \ "error" \ "errorClass"

  private def queryContext(statement: JObject): JObject =
    statement \ "error" \ "queryContext" match {
      case JArray(List(ctx: JObject)) => ctx
      case other => fail(s"expected one query context, got: $other")
    }

  private val emoji = new String(Character.toChars(0x1F600))

  test("ANALYZE does not run CALL, directly or through EXECUTE IMMEDIATE") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      spark.conf.set("spark.sql.catalog.parse_sql_cat", classOf[InMemoryCatalog].getName)
      try {
        val catalog = spark.sessionState.catalogManager.catalog("parse_sql_cat")
          .asInstanceOf[InMemoryCatalog]
        catalog.createProcedure(
          Identifier.of(Array("ns"), "record"), ParseSqlAnalyzeSuite.UnboundRecord)
        ParseSqlAnalyzeSuite.calls.set(0)
        val results = evalBatch(
          """CALL parse_sql_cat.ns.record(1);
            |EXECUTE IMMEDIATE 'CALL parse_sql_cat.ns.record(2)'""".stripMargin)
        assert(results.map(_ \ "parse_success") === List(JBool(true), JBool(true)))
        assert(ParseSqlAnalyzeSuite.calls.get === 0)
        sql("CALL parse_sql_cat.ns.record(3)").collect()
        assert(ParseSqlAnalyzeSuite.calls.get === 1)
      } finally {
        spark.sessionState.catalogManager.reset()
        spark.sessionState.conf.unsetConf("spark.sql.catalog.parse_sql_cat")
      }
    }
  }

  test("ANALYZE inside a SQL script does not see the script's locals") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """BEGIN
          |  DECLARE parse_sql_outer INT DEFAULT 5;
          |  SELECT parse_sql('SELECT parse_sql_outer AS x', 'ANALYZE');
          |END""".stripMargin).head().getString(0)
      val statement = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(statement \ "analyze_success" === JBool(false))
    }
  }

  test("ANALYZE system.session overlays do not change persistent view bodies") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_pv_base") {
        withView("parse_sql_pv") {
          sql("CREATE TABLE parse_sql_pv_base (a INT) USING parquet")
          sql("CREATE VIEW parse_sql_pv AS SELECT a FROM parse_sql_pv_base")
          val results = evalCall(
            """SELECT parse_sql(
              |  'SELECT * FROM parse_sql_pv',
              |  'ANALYZE',
              |  relations => array(named_struct(
              |    'name', 'system.session.parse_sql_pv_base', 'schema', 'z STRING')))
              |""".stripMargin)
          assert(results.head \ "analyze_success" === JBool(true))
          assert(results.head \ "select_list" === typedColumns("a" -> jsonInt))
        }
      }
    }
  }

  test("ANALYZE INSERT into an overlay honors a column list") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val results = evalCall(
        """SELECT parse_sql(
          |  'INSERT INTO t (b, a) VALUES (''x'', 1); INSERT INTO t (b) VALUES (''y'')',
          |  'ANALYZE',
          |  relations => array(named_struct('name', 't', 'schema', 'a INT, b STRING')))
          |""".stripMargin)
      assert(results.map(_ \ "analyze_success") === List(JBool(true), JBool(true)))
    }
  }

  test("ANALYZE script applies the interpreter's local variable rules") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE OR REPLACE v INT;
          |  DECLARE system.session.w INT;
          |  DROP TEMPORARY VARIABLE IF EXISTS u;
          |END""".stripMargin)
      assert(nested(script).map(errorClass) === List(
        JString("INVALID_VARIABLE_DECLARATION.REPLACE_LOCAL_VARIABLE"),
        JString("INVALID_VARIABLE_DECLARATION.QUALIFIED_LOCAL_VARIABLE"),
        JString("UNSUPPORTED_FEATURE.SQL_SCRIPTING_DROP_TEMPORARY_VARIABLE")))
    }
  }

  test("ANALYZE requires constant arguments; PARSE does not") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val query = "SELECT parse_sql(c, '%s') FROM VALUES ('SELECT 1') AS t(c)"
      val e = intercept[AnalysisException](spark.sql(query.format("ANALYZE")).collect())
      assert(e.getCondition === "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
      assert(spark.sql(query.format("PARSE")).collect().length === 1)
    }
  }

  test("ANALYZE error indexes are code points relative to each statement") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val top = evalStatement(s"SELECT '$emoji' AS e, missing_col")
      val topCtx = queryContext(top)
      assert(topCtx \ "startIndex" === JInt(18))
      assert(topCtx \ "stopIndex" === JInt(28))
      assert(topCtx \ "fragment" === JString("missing_col"))

      val script = evalStatement(s"BEGIN SELECT '$emoji' AS e; SELECT missing_col; END")
      val failed = nested(script)(1)
      assert(failed \ "start" === JInt(24))
      val nestedCtx = queryContext(failed)
      assert(nestedCtx \ "objectType" === JString(""))
      assert(nestedCtx \ "startIndex" === JInt(8))
      assert(nestedCtx \ "stopIndex" === JInt(18))
      assert(nestedCtx \ "fragment" === JString("missing_col"))
    }
  }

  test("ANALYZE script conditions must be BOOLEAN") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val script = evalStatement("BEGIN IF 1 THEN SELECT 1; END IF; END")
      val condition = nested(script).head
      assert(condition \ "analyze_success" === JBool(false))
      assert(errorClass(condition) === JString("INVALID_BOOLEAN_STATEMENT"))
    }
  }

  test("ANALYZE binds parameter markers in subqueries and CTEs") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val results = evalCall(
        """SELECT parse_sql(
          |  'SELECT (SELECT :p) AS x; WITH c AS (SELECT :p AS v) SELECT v FROM c',
          |  'ANALYZE',
          |  parameter_markers => array(named_struct('name', 'p', 'type', 'INT')))
          |""".stripMargin)
      assert(results.map(_ \ "analyze_success") === List(JBool(true), JBool(true)))
      assert(results(0) \ "select_list" === typedColumns("x" -> jsonInt))
      assert(results(1) \ "select_list" === typedColumns("v" -> jsonInt))
    }
  }

  test("ANALYZE script types a parameterized cursor query at OPEN") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false") {
      val script = evalStatement(
        """BEGIN
          |  DECLARE c CURSOR FOR SELECT :lo + 1 AS id;
          |  DECLARE d CURSOR FOR SELECT missing_col WHERE :lo > 0;
          |  DECLARE e CURSOR FOR SELECT missing_col;
          |  OPEN c USING 5 AS lo;
          |  OPEN d USING 1 AS lo;
          |  CLOSE c;
          |END""".stripMargin)
      val Seq(declareC, declareD, declareE, openC, openD, closeC) = nested(script)
      Seq(declareC, declareD, openC, closeC).foreach { s =>
        assert(s \ "analyze_success" === JBool(true), s)
      }
      assert(errorClass(declareE) === JString("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION"))
      val declareCtx = queryContext(declareE)
      assert(declareCtx \ "startIndex" === JInt(29))
      assert(declareCtx \ "fragment" === JString("missing_col"))
      assert(errorClass(openD) === JString("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION"))
      val openCtx = queryContext(openD)
      assert(openCtx \ "objectType" === JString("CURSOR"))
      assert(openCtx \ "objectName" === JString("d"))
      assert(openCtx \ "startIndex" === JInt(8))
      assert(openCtx \ "fragment" === JString("missing_col"))
    }
  }
}

object ParseSqlAnalyzeSuite {
  val calls = new AtomicInteger(0)

  object UnboundRecord extends UnboundProcedure {
    override def name: String = "record"
    override def description: String = "counts its invocations"
    override def bind(inputType: StructType): BoundProcedure = Record
  }

  object Record extends BoundProcedure {
    override def name: String = "record"
    override def description: String = "counts its invocations"
    override def isDeterministic: Boolean = false
    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.IntegerType).build())
    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      calls.incrementAndGet()
      Collections.emptyIterator[Scan]()
    }
  }
}
