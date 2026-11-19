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
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, SuppliedRelationRegistry}
import org.apache.spark.sql.connector.catalog.{
  Identifier, InMemoryCatalog, InMemoryRowLevelOperationTableCatalog
}
import org.apache.spark.sql.connector.catalog.procedures.{
  BoundProcedure,
  ProcedureParameter,
  UnboundProcedure
}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.util.Utils

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
  private val jsonString: JObject = JObject(
    "name" -> JString("string"),
    "collation" -> JString("UTF8_BINARY"))

  private val jsonBoolean: JObject = jsonType("boolean")

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
      assert(errorClass === "TABLE_OR_VIEW_NOT_FOUND")
    }
  }

  test("ANALYZE inherits the invoker schema") {
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
        } finally {
          sql("USE parse_sql_ctx_a")
        }
        try {
          val found = evalStatement("SELECT a FROM t")
          assert(found \ "analyze_success" === JBool(true))
          assert(found \ "source_table_references" === JArray(List(JArray(List(
            JString("spark_catalog"), JString("parse_sql_ctx_a"), JString("t"))))))
          assert(spark.sql("SELECT current_schema()").head().getString(0) ===
            "parse_sql_ctx_a")
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

  test("ANALYZE named mode argument") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT 1 AS a',
          |  mode => 'ANALYZE')""".stripMargin).head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(analyzed \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("a"))), "type" -> jsonInt))))
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

  private def evalBatch(sql: String): List[JObject] =
    evalJson("'" + sql.replace("'", "''") + "'") match {
      case JArray(values) => values.map(_.asInstanceOf[JObject])
      case other => fail(s"expected a JSON array, got: $other")
    }

  private def typedColumns(columns: (String, JObject)*): JArray = JArray(columns.map {
    case (name, dataType) => JObject("name" -> JArray(List(JString(name))), "type" -> dataType)
  }.toList)

  private def ref(parts: String*): JArray = JArray(parts.map(JString).toList)

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
        assert(results.map(_ \ "analyze_success") === List(JBool(true), JBool(true)))
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
      val cls = (statement \ "error" \ "errorClass").asInstanceOf[JString].s
      assert(cls === "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION" ||
        cls === "UNRESOLVED_COLUMN.WITH_SUGGESTION" ||
        cls.startsWith("UNRESOLVED_COLUMN"))
    }
  }

  test("ANALYZE requires constant arguments; PARSE does not") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val query = "SELECT parse_sql(c, '%s') FROM VALUES ('SELECT 1') AS t(c)"
      val e = intercept[AnalysisException](spark.sql(query.format("ANALYZE")).collect())
      assert(e.getCondition === "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
      assert(spark.sql(query.format("PARSE")).collect().length === 1)
      val modeCol = intercept[AnalysisException](
        spark.sql("SELECT parse_sql(c, m) FROM VALUES ('SELECT 1', 'PARSE') AS t(c, m)")
          .collect())
      assert(modeCol.getCondition === "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
    }
  }

  test("ANALYZE rejects an argument that does not fold, such as a UDF over literals") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      spark.udf.register("parse_sql_identity", (s: String) => s)
      try {
        val e = intercept[AnalysisException](
          spark.sql("SELECT parse_sql(parse_sql_identity('SELECT 1'), 'ANALYZE')").collect())
        assert(e.getCondition === "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
        val folded = evalCall(
          "SELECT parse_sql('SELECT 1 AS a', 'ANALYZE', NULL, " +
            "concat(current_schema(), ''))")
        assert(folded.head \ "analyze_success" === JBool(true), folded)
      } finally {
        spark.sessionState.catalog.dropTempFunction("parse_sql_identity", ignoreIfNotExists = true)
      }
    }
  }

  test("ANALYZE does not execute SQL scripts") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_script_t", "parse_sql_script_u") {
        sql("CREATE TABLE parse_sql_script_t (a INT) USING parquet")
        sql("CREATE TABLE parse_sql_script_u (a INT) USING parquet")
        val schema = spark.sql("SELECT current_schema()").head().getString(0)
        val j = evalStatement(
          """BEGIN
            |  INSERT INTO parse_sql_script_t VALUES (1);
            |  DROP TABLE parse_sql_script_u;
            |END""".stripMargin)
        assert(j \ "parse_success" === JBool(true))
        assert(j \ "analyze_success" === JBool(false))
        assert(errorClass(j) === JString("PARSE_SQL_ANALYZE_UNSUPPORTED_SCRIPT"))
        assert(spark.table("parse_sql_script_t").count() === 0)
        assert(spark.catalog.tableExists("parse_sql_script_u"))
        assert(spark.sql("SELECT current_schema()").head().getString(0) === schema)
      }
    }
  }

  test("ANALYZE does not execute a SQL script wrapped in named parameter markers") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_wrap_t", "parse_sql_wrap_u") {
        sql("CREATE TABLE parse_sql_wrap_t (a INT) USING parquet")
        sql("CREATE TABLE parse_sql_wrap_u (a INT) USING parquet")
        val json = spark.sql(
          """SELECT parse_sql(
            |  'BEGIN
            |  INSERT INTO parse_sql_wrap_t VALUES (:p);
            |  DROP TABLE parse_sql_wrap_u;
            |END',
            |  'ANALYZE',
            |  parameter_markers => array(named_struct('name', 'p', 'type', 'INT')))
            |""".stripMargin).head().getString(0)
        val j = parse(json) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(j \ "parse_success" === JBool(true))
        assert(j \ "analyze_success" === JBool(false))
        assert(errorClass(j) === JString("PARSE_SQL_ANALYZE_UNSUPPORTED_SCRIPT"))
        assert(spark.table("parse_sql_wrap_t").count() === 0)
        assert(spark.catalog.tableExists("parse_sql_wrap_u"))
      }
    }
  }

  test("ANALYZE overlays match session and catalog names under case-sensitive analysis") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable("parse_sql_cs_t") {
        sql("CREATE TABLE parse_sql_cs_t (a INT) USING parquet")
        withTempView("parse_sql_cs_v") {
          sql("CREATE TEMP VIEW parse_sql_cs_v AS SELECT 1 AS a")
          val sessionJson = spark.sql(
            """SELECT parse_sql(
              |  'SELECT a FROM SYSTEM.SESSION.parse_sql_cs_v',
              |  'ANALYZE',
              |  relations => array(named_struct(
              |    'name', 'system.session.parse_sql_cs_v',
              |    'schema', 'a STRING')))
              |""".stripMargin).head().getString(0)
          val sessionHit = parse(sessionJson) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(sessionHit \ "analyze_success" === JBool(true))
          assert(sessionHit \ "select_list" === JArray(List(
            JObject("name" -> JArray(List(JString("a"))), "type" -> jsonString))))

          val catalogJson = spark.sql(
            """SELECT parse_sql(
              |  'SELECT a FROM spark_catalog.default.parse_sql_cs_t',
              |  'ANALYZE',
              |  relations => array(named_struct(
              |    'name', 'SPARK_CATALOG.default.parse_sql_cs_t',
              |    'schema', 'a STRING')))
              |""".stripMargin).head().getString(0)
          val catalogHit = parse(catalogJson) match {
            case JArray(List(value: JObject)) => value
            case other => fail(s"expected one statement object, got: $other")
          }
          assert(catalogHit \ "analyze_success" === JBool(true))
          assert(catalogHit \ "select_list" === JArray(List(
            JObject("name" -> JArray(List(JString("a"))), "type" -> jsonString))))
        }
      }
    }
  }

  test("ANALYZE qualifies created temp views, table-valued builtins and temp functions") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_q_v") {
        sql("CREATE TEMPORARY FUNCTION parse_sql_q_f(x INT) RETURNS INT RETURN x + 1")
        sql("CREATE TEMPORARY FUNCTION abs(x INT) RETURNS INT RETURN x + 100")
        try {
          val created = evalStatement("CREATE TEMP VIEW parse_sql_q_v AS SELECT 1 AS a")
          assert(created \ "analyze_success" === JBool(true))
          assert(created \ "target_table_references" === JArray(List(
            ref("system", "session", "parse_sql_q_v"))))

          val tvf = evalStatement("SELECT * FROM range(3)")
          assert(tvf \ "function_references" === JArray(List(
            ref("system", "builtin", "range"))))

          val tempFn = evalStatement("SELECT parse_sql_q_f(1) AS y")
          assert(tempFn \ "function_references" === JArray(List(
            ref("system", "session", "parse_sql_q_f"))))

          // Under the default function resolution order a builtin wins over a temp function.
          val shadowed = evalStatement("SELECT abs(-1) AS z")
          assert(shadowed \ "function_references" === JArray(List(
            ref("system", "builtin", "abs"))))
        } finally {
          sql("DROP TEMPORARY FUNCTION IF EXISTS parse_sql_q_f")
          sql("DROP TEMPORARY FUNCTION IF EXISTS abs")
        }
      }
    }
  }

  test("ANALYZE current_path leaves the invoker's SET PATH unchanged") {
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
        assert(analyzed \ "analyze_success" === JBool(true), json)
        assert(spark.sql("SELECT current_path()").head().getString(0) === before)
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
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

  test("ANALYZE current_path does not change the path feature or the invoker's path") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val pathEnabled = spark.conf.get(SQLConf.PATH_ENABLED.key)
      val before = spark.sql("SELECT current_path()").head().getString(0)
      val json = spark.sql(
        """SELECT parse_sql(
          |  'SELECT abs(-1) AS x',
          |  'ANALYZE',
          |  current_path => 'system.builtin')""".stripMargin).head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "analyze_success" === JBool(true))
      assert(spark.conf.get(SQLConf.PATH_ENABLED.key) === pathEnabled)
      assert(spark.sql("SELECT current_path()").head().getString(0) === before)
    }
  }

  test("ANALYZE errors on a bound parameter marker do not report the outer parse_sql call") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql('SELECT * FROM range(10) LIMIT :n', 'ANALYZE',
          |  parameter_markers => array(named_struct('name', 'n', 'type', 'INT')))""".stripMargin)
        .head().getString(0)
      val analyzed = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(analyzed \ "analyze_success" === JBool(false), json)
      assert(analyzed \ "error" \ "errorClass" ===
        JString("INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL"), json)
      // The context must not be the outer parse_sql(...) call text.
      assert((analyzed \ "error" \ "queryContext").children.isEmpty, json)
    }
  }

  test("ANALYZE DDL does not see caller-supplied relations and keeps later statements") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val json = spark.sql(
        """SELECT parse_sql(
          |  'ALTER TABLE parse_sql_ddl_missing RENAME COLUMN a TO b;
          |   SELECT a FROM parse_sql_ddl_missing',
          |  'ANALYZE',
          |  relations => array(named_struct('name', 'parse_sql_ddl_missing', 'schema', 'a INT')))
          |""".stripMargin).head().getString(0)
      val statements = parse(json) match {
        case JArray(values) => values.map(_.asInstanceOf[JObject])
        case other => fail(s"expected a statement array, got: $other")
      }
      assert(statements.length === 2)
      assert(statements(0) \ "parse_success" === JBool(true))
      assert(statements(0) \ "analyze_success" === JBool(false))
      assert(errorClass(statements(0)) === JString("TABLE_OR_VIEW_NOT_FOUND"))
      assert(statements(1) \ "analyze_success" === JBool(true))
    }
  }

  test("ANALYZE DDL on a real table is not replaced by a same-named supplied relation") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_ddl_t") {
        sql("CREATE TABLE parse_sql_ddl_t (a INT) USING parquet")
        val json = spark.sql(
          """SELECT parse_sql(
            |  'ALTER TABLE parse_sql_ddl_t ADD COLUMN b INT',
            |  'ANALYZE',
            |  relations => array(named_struct('name', 'parse_sql_ddl_t', 'schema', 'a STRING')))
            |""".stripMargin).head().getString(0)
        val analyzed = parse(json) match {
          case JArray(List(value: JObject)) => value
          case other => fail(s"expected one statement object, got: $other")
        }
        assert(analyzed \ "analyze_success" === JBool(true), json)
        // The catalog table is the target, not the supplied relation.
        assert(analyzed \ "target_table_references" ===
          JArray(List(JArray(List(
            JString("spark_catalog"), JString("default"), JString("parse_sql_ddl_t"))))), json)
        // ANALYZE does not run the statement.
        assert(spark.table("parse_sql_ddl_t").columns.toSeq === Seq("a"))
      }
    }
  }

  test("ANALYZE reports range as system.builtin and de-duplicates count") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_dedup_t") {
        sql("CREATE TABLE parse_sql_dedup_t (a INT, b INT) USING parquet")
        val rangeStmt = evalStatement("SELECT * FROM range(3)")
        assert(rangeStmt \ "analyze_success" === JBool(true))
        assert(rangeStmt \ "function_references" === JArray(List(
          ref("system", "builtin", "range"))))
        val counts = evalStatement(
          "SELECT count(a), COUNT(b) FROM parse_sql_dedup_t")
        assert(counts \ "analyze_success" === JBool(true))
        assert(counts \ "function_references" === JArray(List(
          ref("system", "builtin", "count"))))
      }
    }
  }

  test("ANALYZE qualifies DDL targets by statement kind") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val tempView = evalStatement("CREATE TEMPORARY VIEW parse_sql_ddl_v AS SELECT 1 AS a")
      assert(tempView \ "analyze_success" === JBool(true))
      assert(tempView \ "target_table_references" === JArray(List(
        ref("system", "session", "parse_sql_ddl_v"))))
      assert(!spark.catalog.tableExists("parse_sql_ddl_v"))
      withTempView("parse_sql_ddl_t") {
        sql("CREATE TEMP VIEW parse_sql_ddl_t AS SELECT 1 AS a")
        val table = evalStatement("CREATE TABLE parse_sql_ddl_t (a INT) USING parquet")
        assert(table \ "analyze_success" === JBool(true))
        assert(table \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ddl_t"))))
      }
    }
  }

  test("ANALYZE types session-catalog CTAS query columns") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_ctas_t") {
        val j = evalStatement(
          "CREATE TABLE parse_sql_ctas_t USING parquet AS SELECT 1 AS a")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "select_list" === typedColumns("a" -> jsonInt))
        assert(!spark.catalog.tableExists("parse_sql_ctas_t"))
      }
    }
  }

  test("ANALYZE does not execute a mixed DDL/DML batch") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_batch_t", "parse_sql_batch_u") {
        sql("CREATE TABLE parse_sql_batch_t (a INT) USING parquet")
        val results = evalBatch(
          """INSERT INTO parse_sql_batch_t VALUES (1);
            |CREATE TABLE parse_sql_batch_u (a INT) USING parquet;
            |DROP TABLE parse_sql_batch_t""".stripMargin)
        assert(results.map(_ \ "analyze_success") ===
          List(JBool(true), JBool(true), JBool(true)))
        assert(spark.table("parse_sql_batch_t").count() === 0)
        assert(!spark.catalog.tableExists("parse_sql_batch_u"))
        assert(spark.catalog.tableExists("parse_sql_batch_t"))
      }
    }
  }

  test("ANALYZE qualifies global temp views and DROP of local temp views") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withGlobalTempView("parse_sql_global_v") {
        sql("CREATE OR REPLACE GLOBAL TEMP VIEW parse_sql_global_v AS SELECT 1 AS a")
        val globalDb = spark.conf.get("spark.sql.globalTempDatabase")
        val j = evalStatement(s"SELECT a FROM $globalDb.parse_sql_global_v")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(
          ref("spark_catalog", globalDb, "parse_sql_global_v"))))
      }
      withTempView("parse_sql_drop_v") {
        sql("CREATE TEMP VIEW parse_sql_drop_v AS SELECT 1 AS a")
        val drop = evalStatement("DROP VIEW parse_sql_drop_v")
        assert(drop \ "analyze_success" === JBool(true))
        assert(drop \ "target_table_references" === JArray(List(
          ref("system", "session", "parse_sql_drop_v"))))
        assert(spark.catalog.tableExists("parse_sql_drop_v"))
      }
    }
  }

  test("ANALYZE types EXPLAIN and DESCRIBE QUERY inner plans") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_explain_t") {
        sql("CREATE TABLE parse_sql_explain_t (a INT) USING parquet")
        val explain = evalStatement("EXPLAIN SELECT a FROM parse_sql_explain_t")
        assert(explain \ "analyze_success" === JBool(true))
        assert(explain \ "select_list" === typedColumns("a" -> jsonInt))
        assert(explain \ "source_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_explain_t"))))
        val missing = evalStatement("EXPLAIN SELECT nope FROM missing_t")
        assert(missing \ "analyze_success" === JBool(false))
        val missingClass = (missing \ "error" \ "errorClass").asInstanceOf[JString].s
        assert(missingClass === "TABLE_OR_VIEW_NOT_FOUND")
        val nested = evalStatement("EXPLAIN EXPLAIN SELECT nope FROM missing_t")
        assert(nested \ "analyze_success" === JBool(false))
      }
      val describe = evalStatement("DESCRIBE QUERY SELECT 1 AS a")
      assert(describe \ "analyze_success" === JBool(true))
      assert(describe \ "select_list" === typedColumns("a" -> jsonInt))
    }
  }

  test("ANALYZE reports path-based datasource tables as format plus path") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).write.mode("overwrite").parquet(path)
        val j = evalStatement(s"SELECT id FROM parquet.`$path`")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(ref("parquet", path))))
      }
    }
  }

  test("ANALYZE unknown functions stay JSON rather than INTERNAL_ERROR") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val j = evalStatement("SELECT no_such_parse_sql_fn(1)")
      assert(j \ "parse_success" === JBool(true))
      assert(j \ "analyze_success" === JBool(false))
      val cls = (j \ "error" \ "errorClass").asInstanceOf[JString].s
      assert(cls === "UNRESOLVED_ROUTINE")
    }
  }

  test("ANALYZE keeps distinct tables that share a name suffix") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_zzdb")
      try {
        withTable("parse_sql_zzdb.t", "t") {
          sql("CREATE TABLE parse_sql_zzdb.t (a INT) USING parquet")
          sql("CREATE TABLE t (a INT) USING parquet")
          val join1 = evalStatement("SELECT * FROM parse_sql_zzdb.t JOIN t")
          assert(join1 \ "analyze_success" === JBool(true))
          assert(join1 \ "source_table_references" === JArray(List(
            ref("spark_catalog", "parse_sql_zzdb", "t"),
            ref("spark_catalog", "default", "t"))))
          val join2 = evalStatement("SELECT * FROM t JOIN parse_sql_zzdb.t")
          assert(join2 \ "analyze_success" === JBool(true))
          assert(join2 \ "source_table_references" === JArray(List(
            ref("spark_catalog", "default", "t"),
            ref("spark_catalog", "parse_sql_zzdb", "t"))))
          val join3 = evalStatement(
            "SELECT * FROM spark_catalog.parse_sql_zzdb.t JOIN t")
          assert(join3 \ "analyze_success" === JBool(true))
          assert(join3 \ "source_table_references" === JArray(List(
            ref("spark_catalog", "parse_sql_zzdb", "t"),
            ref("spark_catalog", "default", "t"))))
        }
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_zzdb CASCADE")
      }
    }
  }

  test("ANALYZE persistent CTAS and CREATE VIEW ignore a same-named temp view") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_tv") {
        sql("CREATE TEMP VIEW parse_sql_tv AS SELECT 1 AS a")
        val ctas = evalStatement(
          "CREATE TABLE parse_sql_tv USING parquet AS SELECT 1 AS a")
        assert(ctas \ "analyze_success" === JBool(true))
        assert(ctas \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_tv"))))
        val view = evalStatement("CREATE VIEW parse_sql_tv AS SELECT 1 AS a")
        assert(view \ "analyze_success" === JBool(true))
        assert(view \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_tv"))))
      }
    }
  }

  test("ANALYZE expands star select lists") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_star_t") {
        sql("CREATE TABLE parse_sql_star_t (a INT) USING parquet")
        val j = evalStatement("SELECT * FROM parse_sql_star_t")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "select_list" === typedColumns("a" -> jsonInt))
      }
    }
  }

  test("ANALYZE keeps session-catalog write and DDL references") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_ref_s", "parse_sql_ref_t", "parse_sql_ref_u") {
        sql("CREATE TABLE parse_sql_ref_s (a INT) USING parquet")
        sql("CREATE TABLE parse_sql_ref_t (a INT) USING parquet")
        val insert = evalStatement(
          "INSERT INTO parse_sql_ref_t SELECT a FROM parse_sql_ref_s")
        assert(insert \ "analyze_success" === JBool(true))
        assert(insert \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ref_t"))))
        assert(insert \ "source_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ref_s"))))
        val ctas = evalStatement(
          "CREATE TABLE parse_sql_ref_u USING parquet AS SELECT a FROM parse_sql_ref_s")
        assert(ctas \ "analyze_success" === JBool(true))
        assert(ctas \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ref_u"))))
        assert(ctas \ "source_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ref_s"))))
        val view = evalStatement(
          "CREATE VIEW parse_sql_ref_v AS SELECT a FROM parse_sql_ref_s")
        assert(view \ "analyze_success" === JBool(true))
        assert(view \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ref_v"))))
        assert(view \ "source_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_ref_s"))))
        assert(!spark.catalog.tableExists("parse_sql_ref_u"))
        assert(!spark.catalog.tableExists("parse_sql_ref_v"))
      }
    }
  }

  test("ANALYZE qualifies session-qualified temp view names") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_sess_v") {
        sql("CREATE TEMP VIEW parse_sql_sess_v AS SELECT 1 AS a")
        val drop = evalStatement("DROP VIEW session.parse_sql_sess_v")
        assert(drop \ "analyze_success" === JBool(true))
        assert(drop \ "target_table_references" === JArray(List(
          ref("system", "session", "parse_sql_sess_v"))))
        val qualified = evalStatement("DROP VIEW system.session.parse_sql_sess_v")
        assert(qualified \ "analyze_success" === JBool(true))
        assert(qualified \ "target_table_references" === JArray(List(
          ref("system", "session", "parse_sql_sess_v"))))
        assert(spark.catalog.tableExists("parse_sql_sess_v"))
      }
    }
  }

  test("ANALYZE does not load a misconfigured catalog while qualifying") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      spark.conf.set("spark.sql.catalog.badcat", "com.example.Missing")
      try {
        val results = evalBatch("SELECT 1 AS a; SELECT * FROM badcat.ns.t")
        assert(results.map(_ \ "parse_success") === List(JBool(true), JBool(true)))
        assert(results.head \ "analyze_success" === JBool(true))
        assert(results(1) \ "analyze_success" === JBool(false))
      } finally {
        spark.conf.unset("spark.sql.catalog.badcat")
      }
    }
  }

  test("ANALYZE statements do not see earlier batch side effects") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val schema = spark.sql("SELECT current_schema()").head().getString(0)
      sql("CREATE DATABASE IF NOT EXISTS parse_sql_iso_db")
      try {
        val results = evalBatch(
          """USE parse_sql_iso_db;
            |CREATE TEMP VIEW parse_sql_iso_v AS SELECT 1 AS a;
            |SELECT * FROM parse_sql_iso_v;
            |SELECT 1 AS a FROM missing_parse_sql_iso;
            |SELECT 1 AS ok""".stripMargin)
        assert(results.map(_ \ "analyze_success") ===
          List(JBool(true), JBool(true), JBool(false), JBool(false), JBool(true)))
        assert(spark.sql("SELECT current_schema()").head().getString(0) === schema)
        assert(!spark.catalog.tableExists("parse_sql_iso_v"))
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_iso_db")
      }
    }
  }

  test("ANALYZE does not run EXECUTE IMMEDIATE DML") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_exec_t") {
        sql("CREATE TABLE parse_sql_exec_t (a INT) USING parquet")
        val j = evalStatement("EXECUTE IMMEDIATE 'INSERT INTO parse_sql_exec_t VALUES (1)'")
        assert(j \ "analyze_success" === JBool(true))
        assert(spark.table("parse_sql_exec_t").count() === 0)
      }
    }
  }

  test("ANALYZE still works when ConstantFolding is excluded") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      val j = evalStatement("SELECT 1 AS a")
      assert(j \ "analyze_success" === JBool(true))
      assert(j \ "select_list" === typedColumns("a" -> jsonInt))
      val scalar = evalCall(
        "SELECT (SELECT parse_sql('SELECT 1 AS a', 'ANALYZE'))")
      assert(scalar.head \ "analyze_success" === JBool(true))
      assert(scalar.head \ "select_list" === typedColumns("a" -> jsonInt))
    }
  }

  test("ANALYZE still works when ConstantFolding and EvaluateParseSql are excluded") {
    // EvaluateParseSql is not excludable, so this must match the test above. The rule is
    // what runs a constant ANALYZE on the driver; if it were dropped, a task would fail.
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          ("org.apache.spark.sql.catalyst.optimizer.ConstantFolding," +
            "org.apache.spark.sql.catalyst.optimizer.EvaluateParseSql")) {
      val j = evalStatement("SELECT 1 AS a")
      assert(j \ "analyze_success" === JBool(true))
      assert(j \ "select_list" === typedColumns("a" -> jsonInt))
    }
  }

  test("PARSE keeps a failing constant in an untaken CASE branch under ANSI") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true") {
      val rows = sql(
        "SELECT CASE WHEN id > 0 THEN parse_sql(CAST(1/0 AS STRING)) END FROM range(1)")
        .collect()
      assert(rows.length === 1)
      assert(rows.head.isNullAt(0))
    }
  }

  test("ANALYZE of a failing constant in an untaken CASE branch does not fail the query") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true") {
      val rows = sql(
        "SELECT CASE WHEN id > 0 THEN parse_sql(CAST(1/0 AS STRING), 'ANALYZE') END " +
          "FROM range(1)").collect()
      assert(rows.length === 1)
      assert(rows.head.isNullAt(0))
    }
  }

  test("ANALYZE attributes a MERGE source to the table it names, not the row-level target") {
    val catalog = "parse_sql_rl"
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        s"spark.sql.catalog.$catalog" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      try {
        sql(s"CREATE NAMESPACE $catalog.prod")
        sql(s"CREATE NAMESPACE $catalog.staging")
        sql(s"CREATE TABLE $catalog.prod.t (id INT, v INT)")
        sql(s"CREATE TABLE $catalog.staging.t (id INT, v INT)")
        sql(s"USE $catalog.staging")
        val j = evalStatement(
          s"""MERGE INTO $catalog.prod.t AS tgt USING t AS src ON tgt.id = src.id
             |WHEN MATCHED THEN DELETE""".stripMargin)
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "target_table_references" === JArray(List(ref(catalog, "prod", "t"))))
        assert(j \ "source_table_references" === JArray(List(ref(catalog, "staging", "t"))))
      } finally {
        sql("USE spark_catalog.default")
        spark.sessionState.catalogManager.reset()
        spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalog")
      }
    }
  }

  test("ANALYZE does not report a SQL table function body as the statement's source") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE IF NOT EXISTS parse_sql_tvf_db")
      try {
        withTable("parse_sql_tvf_db.t", "t") {
          sql("CREATE TABLE parse_sql_tvf_db.t (x INT) USING parquet")
          sql("CREATE TABLE t (x INT) USING parquet")
          withUserDefinedFunction("parse_sql_tvf" -> false) {
            sql("CREATE FUNCTION parse_sql_tvf() RETURNS TABLE (x INT) " +
              "RETURN SELECT x FROM parse_sql_tvf_db.t")
            val j = evalStatement("SELECT * FROM parse_sql_tvf() JOIN t")
            assert(j \ "analyze_success" === JBool(true))
            assert(j \ "source_table_references" === JArray(List(
              ref("spark_catalog", "default", "t"))))
          }
        }
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_tvf_db CASCADE")
      }
    }
  }

  test("ANALYZE reports the stored identity of a table named in another case") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_case_t") {
        sql("CREATE TABLE parse_sql_case_t (a INT) USING parquet")
        val j = evalStatement("SELECT a FROM PARSE_SQL_CASE_T")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_case_t"))))
      }
    }
  }

  test("ANALYZE reports relative path-based datasource tables as format plus path") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val dir = s"parse_sql_rel_${java.util.UUID.randomUUID()}"
      try {
        spark.range(1).write.mode("overwrite").parquet(dir)
        val j = evalStatement(s"SELECT id FROM parquet.`$dir`")
        assert(j \ "analyze_success" === JBool(true))
        assert(j \ "source_table_references" === JArray(List(ref("parquet", dir))))
      } finally {
        Utils.deleteRecursively(new java.io.File(dir))
      }
    }
  }

  test("ANALYZE reports the owner of a routine that a temp function shadows") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      // The temp and persistent functions share a name, so clean up both explicitly.
      try {
        sql("CREATE FUNCTION parse_sql_ord_f() RETURNS INT RETURN 1")
        sql("CREATE TEMPORARY FUNCTION parse_sql_ord_f() RETURNS INT RETURN 2")
        val second = evalStatement("SELECT parse_sql_ord_f()")
        assert(second \ "function_references" === JArray(List(
          ref("system", "session", "parse_sql_ord_f"))))
        withSQLConf(SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "last") {
          // With `last`, the persistent function in the current schema is found first.
          val last = evalStatement("SELECT parse_sql_ord_f()")
          assert(last \ "function_references" === JArray(List(
            ref("spark_catalog", "default", "parse_sql_ord_f"))))
        }
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS parse_sql_ord_f")
        sql("DROP FUNCTION IF EXISTS parse_sql_ord_f")
      }
    }
  }

  test("ANALYZE reports statement-relative code points for an error after an emoji") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      // The emoji is one code point, so `nope` starts at code point 13 (1-based).
      val emoji = new String(Character.toChars(0x1F600))
      val j = evalStatement(s"SELECT '$emoji', nope")
      assert(j \ "analyze_success" === JBool(false))
      val context = (j \ "error" \ "queryContext")(0)
      assert(context \ "startIndex" === JInt(13))
      assert(context \ "stopIndex" === JInt(16))
      assert(context \ "fragment" === JString("nope"))
    }
  }

  test("ANALYZE reports CHAR and VARCHAR lengths in select_list") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_cv") {
        sql("CREATE TABLE parse_sql_cv (c CHAR(3), v VARCHAR(10)) USING parquet")
        val j = evalStatement("SELECT c, v FROM parse_sql_cv")
        assert(j \ "select_list" === JArray(List(
          JObject("name" -> ref("c"),
            "type" -> JObject("name" -> JString("char"), "length" -> JInt(3))),
          JObject("name" -> ref("v"),
            "type" -> JObject("name" -> JString("varchar"), "length" -> JInt(10))))))
      }
    }
  }

  test("ANALYZE of row-level DELETE, UPDATE and MERGE reports no rewrite columns") {
    val catalog = "parse_sql_rl2"
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        s"spark.sql.catalog.$catalog" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      try {
        sql(s"CREATE NAMESPACE $catalog.ns")
        sql(s"CREATE TABLE $catalog.ns.rl (id INT, v INT)")
        sql(s"CREATE TABLE $catalog.ns.src (id INT, v INT)")
        val statements = Seq(
          s"DELETE FROM $catalog.ns.rl WHERE id = 1",
          s"UPDATE $catalog.ns.rl SET v = 2 WHERE id = 1",
          s"MERGE INTO $catalog.ns.rl AS t USING $catalog.ns.src AS s ON t.id = s.id " +
            "WHEN MATCHED THEN DELETE")
        statements.foreach { statement =>
          val j = evalStatement(statement)
          assert(j \ "analyze_success" === JBool(true), statement)
          assert(j \ "target_table_references" === JArray(List(ref(catalog, "ns", "rl"))))
          assert(j \ "select_list" === JNothing, statement)
        }
      } finally {
        spark.sessionState.catalogManager.reset()
        spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalog")
      }
    }
  }


  test("ANALYZE reports temp view targets for global, USING and EXPLAIN forms") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val globalDb = spark.sharedState.globalTempDB
      val asGlobal = evalStatement("CREATE GLOBAL TEMP VIEW parse_sql_gtv AS SELECT 1 AS a")
      assert(asGlobal \ "target_table_references" === JArray(List(
        ref("spark_catalog", globalDb, "parse_sql_gtv"))))
      val usingLocal = evalStatement(
        "CREATE TEMPORARY VIEW parse_sql_tu USING parquet OPTIONS (path '/tmp/parse_sql_tu')")
      assert(usingLocal \ "analyze_success" === JBool(true))
      assert(usingLocal \ "target_table_references" === JArray(List(
        ref("system", "session", "parse_sql_tu"))))
      val usingGlobal = evalStatement(
        "CREATE GLOBAL TEMPORARY VIEW parse_sql_tug USING parquet " +
          "OPTIONS (path '/tmp/parse_sql_tug')")
      assert(usingGlobal \ "target_table_references" === JArray(List(
        ref("spark_catalog", globalDb, "parse_sql_tug"))))
      val explain = evalStatement("EXPLAIN CREATE TEMPORARY VIEW parse_sql_etv AS SELECT 1 AS a")
      assert(explain \ "analyze_success" === JBool(true))
      assert(explain \ "target_table_references" === JArray(List(
        ref("system", "session", "parse_sql_etv"))))
      assert(!spark.catalog.tableExists("parse_sql_gtv"))
      assert(!spark.catalog.tableExists("parse_sql_etv"))
    }
  }

  test("ANALYZE does not report a SQL scalar function body as the statement's source") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE IF NOT EXISTS parse_sql_fn_db")
      try {
        withTable("parse_sql_fn_db.t", "t") {
          sql("CREATE TABLE parse_sql_fn_db.t (x INT) USING parquet")
          sql("CREATE TABLE t (x INT) USING parquet")
          withUserDefinedFunction("parse_sql_fn_f" -> false) {
            sql("CREATE FUNCTION parse_sql_fn_f() RETURNS INT " +
              "RETURN (SELECT max(x) FROM parse_sql_fn_db.t)")
            val j = evalStatement("SELECT parse_sql_fn_f() FROM t")
            assert(j \ "analyze_success" === JBool(true))
            assert(j \ "source_table_references" === JArray(List(
              ref("spark_catalog", "default", "t"))))
            assert(j \ "function_references" === JArray(List(
              ref("spark_catalog", "default", "parse_sql_fn_f"))))
          }
        }
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_fn_db CASCADE")
      }
    }
  }

  test("ANALYZE does not report a view body as the statement's source") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE IF NOT EXISTS parse_sql_vw_db")
      try {
        withTable("parse_sql_vw_db.t", "t") {
          sql("CREATE TABLE parse_sql_vw_db.t (x INT) USING parquet")
          sql("CREATE TABLE t (x INT) USING parquet")
          withView("parse_sql_vw_v") {
            sql("CREATE VIEW parse_sql_vw_v AS SELECT x FROM parse_sql_vw_db.t")
            val j = evalStatement("SELECT * FROM parse_sql_vw_v JOIN t")
            assert(j \ "analyze_success" === JBool(true))
            assert(j \ "source_table_references" === JArray(List(
              ref("spark_catalog", "default", "parse_sql_vw_v"),
              ref("spark_catalog", "default", "t"))))
          }
        }
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_vw_db CASCADE")
      }
    }
  }

  test("ANALYZE qualifies CACHE and UNCACHE TABLE targets") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_cache_t") {
        sql("CREATE TABLE parse_sql_cache_t (a INT) USING parquet")
        val cache = evalStatement("CACHE TABLE parse_sql_cache_t")
        assert(cache \ "analyze_success" === JBool(true))
        assert(cache \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_cache_t"))))
        val uncache = evalStatement("UNCACHE TABLE parse_sql_cache_t")
        assert(uncache \ "target_table_references" === JArray(List(
          ref("spark_catalog", "default", "parse_sql_cache_t"))))
      }
    }
  }

  test("ANALYZE qualifies CREATE VIEW and ALTER VIEW sources and targets") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_vs_t") {
        sql("CREATE TABLE parse_sql_vs_t (a INT) USING parquet")
        withView("parse_sql_vs_v") {
          sql("CREATE VIEW parse_sql_vs_v AS SELECT a FROM parse_sql_vs_t")
          val create = evalStatement("CREATE VIEW parse_sql_vs_w AS SELECT a FROM PARSE_SQL_VS_T")
          assert(create \ "analyze_success" === JBool(true))
          assert(create \ "target_table_references" === JArray(List(
            ref("spark_catalog", "default", "parse_sql_vs_w"))))
          assert(create \ "source_table_references" === JArray(List(
            ref("spark_catalog", "default", "parse_sql_vs_t"))))
          val alter = evalStatement("ALTER VIEW parse_sql_vs_v AS SELECT a FROM parse_sql_vs_t")
          assert(alter \ "analyze_success" === JBool(true))
          assert(alter \ "target_table_references" === JArray(List(
            ref("spark_catalog", "default", "parse_sql_vs_v"))))
          assert(alter \ "source_table_references" === JArray(List(
            ref("spark_catalog", "default", "parse_sql_vs_t"))))
        }
      }
    }
  }

  test("ANALYZE of row-level DELETE and insert-only MERGE keeps real source reads") {
    val catalog = "parse_sql_rl3"
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        s"spark.sql.catalog.$catalog" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      try {
        sql(s"CREATE NAMESPACE $catalog.ns")
        sql(s"CREATE TABLE $catalog.ns.rl (id INT, v INT)")
        sql(s"CREATE TABLE $catalog.ns.src (id INT, v INT)")
        val delete = evalStatement(
          s"DELETE FROM $catalog.ns.rl WHERE id IN (SELECT id FROM $catalog.ns.src)")
        assert(delete \ "analyze_success" === JBool(true))
        assert(delete \ "target_table_references" === JArray(List(ref(catalog, "ns", "rl"))))
        assert(delete \ "source_table_references" === JArray(List(ref(catalog, "ns", "src"))))
        val self = evalStatement(
          s"DELETE FROM $catalog.ns.rl WHERE id IN (SELECT id FROM $catalog.ns.rl)")
        assert(self \ "source_table_references" === JArray(List(ref(catalog, "ns", "rl"))))
        val insertOnly = evalStatement(
          s"MERGE INTO $catalog.ns.rl AS t USING $catalog.ns.src AS s ON t.id = s.id " +
            "WHEN NOT MATCHED THEN INSERT *")
        assert(insertOnly \ "analyze_success" === JBool(true))
        assert(insertOnly \ "source_table_references" === JArray(List(ref(catalog, "ns", "src"))))
        assert(insertOnly \ "select_list" === typedColumns("id" -> jsonInt, "v" -> jsonInt))
      } finally {
        spark.sessionState.catalogManager.reset()
        spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalog")
      }
    }
  }

  test("ANALYZE of a constant whose SQL text depends on a special datetime value") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true", SQLConf.ANSI_ENABLED.key -> "false") {
      // The text is built from CAST('epoch' AS DATE), which only becomes a date after
      // SpecialDatetimeValues runs, so the ANALYZE call must run after Finish Analysis.
      val json = spark.sql(
        "SELECT parse_sql(concat('SELECT DATE''', CAST(CAST('epoch' AS DATE) AS STRING), " +
          "''' AS d'), 'ANALYZE')").head().getString(0)
      val statement = parse(json) match {
        case JArray(List(value: JObject)) => value
        case other => fail(s"expected one statement object, got: $other")
      }
      assert(statement \ "analyze_success" === JBool(true))
      assert(statement \ "select_list" === typedColumns("d" -> jsonType("date")))
    }
  }

  test("ANALYZE current_schema applies to one call and leaves the session unchanged") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_per_call_a")
      sql("CREATE DATABASE parse_sql_per_call_b")
      try {
        sql("CREATE TABLE parse_sql_per_call_a.t (a INT) USING parquet")
        sql("CREATE TABLE parse_sql_per_call_b.t (a STRING) USING parquet")
        val before = spark.catalog.currentDatabase
        val inA = evalCall(
          "SELECT parse_sql('SELECT a FROM t', 'ANALYZE', NULL, 'parse_sql_per_call_a')")
        val inB = evalCall(
          "SELECT parse_sql('SELECT a FROM t', 'ANALYZE', NULL, 'parse_sql_per_call_b')")
        assert(inA.head \ "analyze_success" === JBool(true))
        assert(inA.head \ "source_table_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_a", "t"))))
        assert(inA.head \ "select_list" === typedColumns("a" -> jsonInt))
        assert(inB.head \ "analyze_success" === JBool(true))
        assert(inB.head \ "source_table_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_b", "t"))))
        assert(inB.head \ "select_list" === typedColumns("a" -> jsonString))
        assert(spark.catalog.currentDatabase === before)
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_a CASCADE")
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_b CASCADE")
      }
    }
  }

  test("ANALYZE of a missing current_schema is an error and keeps the session") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val before = spark.catalog.currentDatabase
      intercept[NoSuchNamespaceException] {
        spark.sql("SELECT parse_sql('SELECT a FROM t', 'ANALYZE', NULL, " +
          "'parse_sql_per_call_missing')").collect()
      }
      assert(spark.catalog.currentDatabase === before)
    }
  }

  test("ANALYZE current_schema does not change the invoker's later resolution") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_per_call_c")
      try {
        sql("CREATE TABLE parse_sql_per_call_c.t (a INT) USING parquet")
        val overridden = evalCall(
          "SELECT parse_sql('SELECT a FROM t', 'ANALYZE', NULL, 'parse_sql_per_call_c')")
        assert(overridden.head \ "analyze_success" === JBool(true))
        // A later call with no context still resolves against the invoker's database.
        val inherited = evalStatement("SELECT a FROM t")
        assert(inherited \ "analyze_success" === JBool(false))
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_c CASCADE")
      }
    }
  }

  test("ANALYZE current_catalog matches the session catalog in any case") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_per_call_d")
      try {
        sql("CREATE TABLE parse_sql_per_call_d.t (a INT) USING parquet")
        val result = evalCall(
          "SELECT parse_sql('SELECT a FROM t; SELECT a FROM parse_sql_missing_t', " +
            "'ANALYZE', 'SPARK_CATALOG', 'parse_sql_per_call_d')")
        assert(result.head \ "analyze_success" === JBool(true))
        assert(result.head \ "source_table_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_d", "t"))))
        // A statement that fails analysis is still qualified with the call's schema.
        assert(result(1) \ "analyze_success" === JBool(false))
        assert(result(1) \ "source_table_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_d", "parse_sql_missing_t"))))
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_d CASCADE")
      }
    }
  }

  test("ANALYZE current_schema qualifies persistent function references with the call") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_per_call_e")
      try {
        sql("CREATE FUNCTION parse_sql_per_call_e.parse_sql_f() RETURNS INT RETURN 1")
        val result = evalCall(
          "SELECT parse_sql('SELECT parse_sql_f() AS x', 'ANALYZE', NULL, " +
            "'parse_sql_per_call_e')")
        assert(result.head \ "analyze_success" === JBool(true), result)
        assert(result.head \ "function_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_e", "parse_sql_f"))))
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_e CASCADE")
      }
    }
  }

  test("ANALYZE current_path expands PATH to the invoker's path") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.PATH_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_per_call_f")
      try {
        sql("CREATE TABLE parse_sql_per_call_f.parse_sql_path_t (a INT) USING parquet")
        sql("SET PATH = spark_catalog.parse_sql_per_call_f, system.builtin")
        val result = evalCall(
          "SELECT parse_sql('SELECT a FROM parse_sql_path_t', 'ANALYZE', " +
            "current_path => 'PATH, system.session')")
        assert(result.head \ "analyze_success" === JBool(true), result)
        assert(result.head \ "source_table_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_f", "parse_sql_path_t"))))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_f CASCADE")
      }
    }
  }

  test("ANALYZE current_path qualifies a persistent function found on the path") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("CREATE DATABASE parse_sql_per_call_g")
      try {
        sql("CREATE FUNCTION parse_sql_per_call_g.parse_sql_g() RETURNS INT RETURN 1")
        val result = evalCall(
          "SELECT parse_sql('SELECT parse_sql_g() AS x', 'ANALYZE', " +
            "current_path => 'system.builtin, SPARK_CATALOG.PARSE_SQL_PER_CALL_G')")
        assert(result.head \ "analyze_success" === JBool(true), result)
        assert(result.head \ "function_references" === JArray(List(
          ref("spark_catalog", "parse_sql_per_call_g", "parse_sql_g"))))
      } finally {
        sql("DROP DATABASE IF EXISTS parse_sql_per_call_g CASCADE")
      }
    }
  }

  test("ANALYZE current_path orders a temp function before a builtin of the same name") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      spark.udf.register("upper", (s: String) => s.length)
      try {
        val builtinFirst = evalCall(
          "SELECT parse_sql('SELECT upper(''a'') AS x', 'ANALYZE', " +
            "current_path => 'system.builtin, system.session')")
        assert(builtinFirst.head \ "select_list" === typedColumns("x" -> jsonString),
          builtinFirst)
        assert(builtinFirst.head \ "function_references" === JArray(List(
          ref("system", "builtin", "upper"))))
        val sessionFirst = evalCall(
          "SELECT parse_sql('SELECT upper(''a'') AS x', 'ANALYZE', " +
            "current_path => 'system.session, system.builtin')")
        assert(sessionFirst.head \ "select_list" === typedColumns("x" -> jsonInt),
          sessionFirst)
        assert(sessionFirst.head \ "function_references" === JArray(List(
          ref("system", "session", "upper"))))
      } finally {
        spark.sessionState.catalog.dropTempFunction("upper", ignoreIfNotExists = true)
      }
    }
  }

  test("ANALYZE overlays do not replace a temp view read by a temp view") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTempView("parse_sql_tv_base", "parse_sql_tv_outer") {
        sql("CREATE TEMPORARY VIEW parse_sql_tv_base AS SELECT 1 AS a")
        sql("CREATE TEMPORARY VIEW parse_sql_tv_outer AS SELECT * FROM parse_sql_tv_base")
        val result = evalCall(
          "SELECT parse_sql('SELECT * FROM parse_sql_tv_outer; SELECT * FROM parse_sql_tv_base', " +
            "'ANALYZE', relations => array(named_struct(" +
            "'name', 'system.session.parse_sql_tv_base', 'schema', 'b STRING')))")
        assert(result.head \ "select_list" === typedColumns("a" -> jsonInt), result)
        assert(result(1) \ "select_list" === typedColumns("b" -> jsonString), result)
      }
    }
  }

  test("ANALYZE overlays do not replace a session variable read by a temp view") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      sql("DECLARE VARIABLE parse_sql_view_var INT DEFAULT 1")
      try {
        withTempView("parse_sql_var_v") {
          sql("CREATE TEMPORARY VIEW parse_sql_var_v AS SELECT parse_sql_view_var AS x")
          val result = evalCall(
            "SELECT parse_sql('SELECT x FROM parse_sql_var_v; SELECT parse_sql_view_var AS y', " +
              "'ANALYZE', variables => array(named_struct(" +
              "'name', 'parse_sql_view_var', 'type', 'STRING')))")
          assert(result.head \ "select_list" === typedColumns("x" -> jsonInt), result)
          assert(result(1) \ "select_list" === typedColumns("y" -> jsonString), result)
        }
      } finally {
        sql("DROP TEMPORARY VARIABLE IF EXISTS parse_sql_view_var")
      }
    }
  }

  test("ANALYZE overlays do not replace a table read by a SQL function body") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_fn_base") {
        withUserDefinedFunction("parse_sql_fn" -> false) {
          sql("CREATE TABLE parse_sql_fn_base (a INT) USING parquet")
          sql("CREATE FUNCTION parse_sql_fn() RETURNS TABLE (a INT) " +
            "RETURN SELECT a FROM parse_sql_fn_base")
          val result = evalCall(
            "SELECT parse_sql('SELECT a FROM parse_sql_fn()', 'ANALYZE', " +
              "relations => array(named_struct(" +
              "'name', 'spark_catalog.default.parse_sql_fn_base', 'schema', 'b STRING')))")
          assert(result.head \ "analyze_success" === JBool(true), result)
          assert(result.head \ "select_list" === typedColumns("a" -> jsonInt), result)
        }
      }
    }
  }

  test("ANALYZE overlays do not replace a table read by a scalar SQL function body") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_sfn_base") {
        withUserDefinedFunction("parse_sql_sfn" -> false) {
          sql("CREATE TABLE parse_sql_sfn_base (a INT) USING parquet")
          sql("CREATE FUNCTION parse_sql_sfn() RETURNS INT " +
            "RETURN (SELECT max(a) FROM parse_sql_sfn_base)")
          val result = evalCall(
            "SELECT parse_sql('SELECT parse_sql_sfn() AS x', 'ANALYZE', " +
              "relations => array(named_struct(" +
              "'name', 'spark_catalog.default.parse_sql_sfn_base', 'schema', 'b STRING')))")
          assert(result.head \ "analyze_success" === JBool(true), result)
          assert(result.head \ "select_list" === typedColumns("x" -> jsonInt), result)
        }
      }
    }
  }

  test("ANALYZE overlays apply to the statement an EXECUTE IMMEDIATE runs") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val result = evalCall(
        "SELECT parse_sql('EXECUTE IMMEDIATE ''SELECT b FROM parse_sql_ei_t''', 'ANALYZE', " +
          "relations => array(named_struct(" +
          "'name', 'spark_catalog.default.parse_sql_ei_t', 'schema', 'b STRING')))")
      assert(result.head \ "analyze_success" === JBool(true), result)
    }
  }

  test("ANALYZE overlays do not replace a table read by a persistent view") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      withTable("parse_sql_view_base") {
        withView("parse_sql_evolving_v") {
          sql("CREATE TABLE parse_sql_view_base (a INT) USING parquet")
          sql("CREATE VIEW parse_sql_evolving_v WITH SCHEMA EVOLUTION AS " +
            "SELECT * FROM parse_sql_view_base")
          val result = evalCall(
            "SELECT parse_sql('SELECT * FROM parse_sql_evolving_v', 'ANALYZE', " +
              "relations => array(named_struct(" +
              "'name', 'spark_catalog.default.parse_sql_view_base', " +
              "'schema', 'b STRING')))")
          assert(result.head \ "analyze_success" === JBool(true), result)
          assert(result.head \ "select_list" === typedColumns("a" -> jsonInt), result)
          // A view WITH SCHEMA EVOLUTION would write an overlay's schema to the catalog.
          val stored = spark.sessionState.catalog.getTableMetadata(
            TableIdentifier("parse_sql_evolving_v"))
          assert(stored.schema.fieldNames.toSeq === Seq("a"))
        }
      }
    }
  }

  test("supplied relation identities are canonical under case-sensitive analysis") {
    withSQLConf(
        SQLConf.PARSE_SQL_ENABLED.key -> "true",
        SQLConf.CASE_SENSITIVE.key -> "true") {
      val mgr = spark.sessionState.catalogManager
      val globalDb = spark.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE)
      assert(SuppliedRelationRegistry.canonicalize(mgr, Seq("SYSTEM", "SESSION", "v")) ===
        Seq("system", "session", "v"))
      assert(SuppliedRelationRegistry.canonicalize(mgr, Seq("System", "Session", "v")) ===
        SuppliedRelationRegistry.sessionIdentity("v"))
      assert(SuppliedRelationRegistry.canonicalize(mgr, Seq("SPARK_CATALOG", "default", "t")) ===
        Seq("spark_catalog", "default", "t"))
      assert(SuppliedRelationRegistry.canonicalize(mgr, Seq("GLOBAL_TEMP", "v")) ===
        Seq(globalDb, "v"))
      // Idempotent, so a stored identity and a looked-up identity agree.
      val once = SuppliedRelationRegistry.canonicalize(mgr, Seq("SPARK_CATALOG", "default", "t"))
      assert(SuppliedRelationRegistry.canonicalize(mgr, once) === once)
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
        assert(analyzed \ "analyze_success" === JBool(true), json)
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
          assert(analyzed \ "analyze_success" === JBool(true), json)
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
            assert(analyzed \ "analyze_success" === JBool(true), json)
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
          |  'SELECT proc.parm1 AS x; SELECT parm1 AS x',
          |  'ANALYZE',
          |  variables => array(named_struct('name', 'proc.parm1', 'type', 'STRING')))
          |""".stripMargin).head().getString(0)
      val (hit, miss) = parse(json) match {
        case JArray(List(first: JObject, second: JObject)) => (first, second)
        case other => fail(s"expected two statement objects, got: $other")
      }
      assert(hit \ "analyze_success" === JBool(true), json)
      assert(hit \ "select_list" === JArray(List(
        JObject("name" -> JArray(List(JString("x"))), "type" -> jsonString))), json)
      assert(miss \ "analyze_success" === JBool(false), json)
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

  test("ANALYZE reports a variable name with more than three parts as JSON") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val result = evalCall(
        "SELECT parse_sql('SELECT 1 AS a', 'ANALYZE', variables => array(named_struct(" +
          "'name', 'a.system.session.v', 'type', 'INT')))")
      assert(result.head \ "analyze_success" === JBool(false))
      assert(result.head \ "error" \ "errorClass" ===
        JString("INVALID_PARAMETER_VALUE.PARSE_SQL_CONTEXT"))
    }
  }

  test("ANALYZE resolves UPDATE, DELETE, and MERGE targets to a supplied relation") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val target = JArray(List(ref("spark_catalog", "default", "parse_sql_ovl")))
      def analyze(stmt: String): JObject = evalCall(
        s"SELECT parse_sql('$stmt', 'ANALYZE', relations => array(named_struct(" +
          "'name', 'spark_catalog.default.parse_sql_ovl', 'schema', 'a INT, b STRING')))").head
      Seq(
        "UPDATE parse_sql_ovl SET a = 1 WHERE b = ''x''",
        "DELETE FROM parse_sql_ovl WHERE a = 1",
        "MERGE INTO parse_sql_ovl USING (SELECT 1 AS a) s ON parse_sql_ovl.a = s.a " +
          "WHEN MATCHED THEN UPDATE SET b = ''y''").foreach { stmt =>
        val result = analyze(stmt)
        assert(result \ "analyze_success" === JBool(true), result)
        assert(result \ "target_table_references" === target, result)
      }
      val missing = analyze("UPDATE parse_sql_ovl SET c = 1")
      assert(missing \ "analyze_success" === JBool(false))
      assert(missing \ "error" \ "errorClass" ===
        JString("UNRESOLVED_COLUMN.WITH_SUGGESTION"))
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
