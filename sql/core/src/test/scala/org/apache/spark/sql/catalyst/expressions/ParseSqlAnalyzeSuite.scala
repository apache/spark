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

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

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

  test("ANALYZE types selected columns and PARSE does not") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val analyzed = evalStatement("SELECT 1 AS a", "ANALYZE")
      assert(analyzed \ "parse_success" === JBool(true))
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")))))

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
          JObject("name" -> JArray(List(JString("c"))), "type" -> JString("BIGINT")))))
      }
    }
  }

  test("ANALYZE leaves temp view names unqualified") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_analyze_v") {
        sql("CREATE OR REPLACE TEMP VIEW parse_sql_analyze_v AS SELECT 1 AS a, 'x' AS b")
        val j = evalStatement("SELECT a, b FROM parse_sql_analyze_v")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" ===
          JArray(List(JArray(List(JString("parse_sql_analyze_v"))))))
        assert(j \ "select_list" === JArray(List(
          JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")),
          JObject("name" -> JArray(List(JString("b"))), "type" -> JString("STRING")))))
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
        JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")))))
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
          JObject("name" -> JArray(List(JString("x"))), "type" -> JString("INT")))))
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
            JObject("name" -> JArray(List(JString("x"))), "type" -> JString("STRING")))))
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
              JObject("name" -> JArray(List(JString("x"))), "type" -> JString("STRING")))))
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
          JObject("name" -> JArray(List(JString("x"))), "type" -> JString("INT")))))
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
        JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")),
        JObject("name" -> JArray(List(JString("b"))), "type" -> JString("STRING")))))
      assert(analyzed \ "source_table_references" === JArray(List(JArray(List(
        JString("spark_catalog"), JString("default"), JString("t"))))))
    }
  }

  test("ANALYZE overlays supplied relations after temp views and before tables") {
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
            JObject("name" -> JArray(List(JString("a"))), "type" -> JString("STRING")))))
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
              JObject("name" -> JArray(List(JString("a"))), "type" -> JString("BOOLEAN")))))
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
          JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")))))

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
              JObject("name" -> JArray(List(JString("a"))), "type" -> JString("INT")))))
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
}
