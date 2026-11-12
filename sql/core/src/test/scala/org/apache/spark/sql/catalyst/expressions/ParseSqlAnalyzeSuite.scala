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
import org.apache.spark.sql.connector.catalog.{
  Identifier, InMemoryCatalog, InMemoryRowLevelOperationTableCatalog
}
import org.apache.spark.sql.connector.catalog.procedures.{
  BoundProcedure,
  ProcedureParameter,
  UnboundProcedure
}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.internal.SQLConf
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
