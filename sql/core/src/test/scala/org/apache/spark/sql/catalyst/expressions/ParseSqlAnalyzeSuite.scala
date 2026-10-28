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

  test("ANALYZE requires constant arguments; PARSE does not") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val query = "SELECT parse_sql(c, '%s') FROM VALUES ('SELECT 1') AS t(c)"
      val e = intercept[AnalysisException](spark.sql(query.format("ANALYZE")).collect())
      assert(e.getCondition === "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
      assert(spark.sql(query.format("PARSE")).collect().length === 1)
    }
  }

  test("ANALYZE error indexes are code points relative to the statement") {
    withSQLConf(SQLConf.PARSE_SQL_ENABLED.key -> "true") {
      val top = evalStatement(s"SELECT '$emoji' AS e, missing_col")
      val topCtx = queryContext(top)
      assert(topCtx \ "startIndex" === JInt(18))
      assert(topCtx \ "stopIndex" === JInt(28))
      assert(topCtx \ "fragment" === JString("missing_col"))
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
