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

package org.apache.spark.sql.execution

import scala.collection.JavaConverters._

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, Concat, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, RefreshResource}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.StringType

/**
 * Parser test cases for rules defined in [[SparkSqlParser]].
 *
 * See [[org.apache.spark.sql.catalyst.parser.PlanParserSuite]] for rules
 * defined in the Catalyst module.
 */
class SparkSqlParserSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private lazy val parser = new SparkSqlParser()

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parser.parsePlan(sqlCommand), plan)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parser.parsePlan)(sqlCommand, messages: _*)

  test("Checks if SET/RESET can parse all the configurations") {
    // Force to build static SQL configurations
    StaticSQLConf
    ConfigEntry.knownConfigs.values.asScala.foreach { config =>
      assertEqual(s"SET ${config.key}", SetCommand(Some(config.key -> None)))
      assertEqual(s"SET `${config.key}`", SetCommand(Some(config.key -> None)))

      val defaultValueStr = config.defaultValueString
      if (config.defaultValue.isDefined && defaultValueStr != null) {
        assertEqual(s"SET ${config.key}=`$defaultValueStr`",
          SetCommand(Some(config.key -> Some(defaultValueStr))))
        assertEqual(s"SET `${config.key}`=`$defaultValueStr`",
          SetCommand(Some(config.key -> Some(defaultValueStr))))

        if (!defaultValueStr.contains(";")) {
          assertEqual(s"SET ${config.key}=$defaultValueStr",
            SetCommand(Some(config.key -> Some(defaultValueStr))))
          assertEqual(s"SET `${config.key}`=$defaultValueStr",
            SetCommand(Some(config.key -> Some(defaultValueStr))))
        }
      }
      assertEqual(s"RESET ${config.key}", ResetCommand(Some(config.key)))
    }
  }

  test("Report Error for invalid usage of SET command") {
    assertEqual("SET", SetCommand(None))
    assertEqual("SET -v", SetCommand(Some("-v", None)))
    assertEqual("SET spark.sql.key", SetCommand(Some("spark.sql.key" -> None)))
    assertEqual("SET  spark.sql.key   ", SetCommand(Some("spark.sql.key" -> None)))
    assertEqual("SET spark:sql:key=false", SetCommand(Some("spark:sql:key" -> Some("false"))))
    assertEqual("SET spark:sql:key=", SetCommand(Some("spark:sql:key" -> Some(""))))
    assertEqual("SET spark:sql:key=  ", SetCommand(Some("spark:sql:key" -> Some(""))))
    assertEqual("SET spark:sql:key=-1 ", SetCommand(Some("spark:sql:key" -> Some("-1"))))
    assertEqual("SET spark:sql:key = -1", SetCommand(Some("spark:sql:key" -> Some("-1"))))
    assertEqual("SET 1.2.key=value", SetCommand(Some("1.2.key" -> Some("value"))))
    assertEqual("SET spark.sql.3=4", SetCommand(Some("spark.sql.3" -> Some("4"))))
    assertEqual("SET 1:2:key=value", SetCommand(Some("1:2:key" -> Some("value"))))
    assertEqual("SET spark:sql:3=4", SetCommand(Some("spark:sql:3" -> Some("4"))))
    assertEqual("SET 5=6", SetCommand(Some("5" -> Some("6"))))
    assertEqual("SET spark:sql:key = va l u  e ",
      SetCommand(Some("spark:sql:key" -> Some("va l u  e"))))
    assertEqual("SET `spark.sql.    key`=value",
      SetCommand(Some("spark.sql.    key" -> Some("value"))))
    assertEqual("SET `spark.sql.    key`= v  a lu e ",
      SetCommand(Some("spark.sql.    key" -> Some("v  a lu e"))))
    assertEqual("SET `spark.sql.    key`=  -1",
      SetCommand(Some("spark.sql.    key" -> Some("-1"))))
    assertEqual("SET key=", SetCommand(Some("key" -> Some(""))))

    val expectedErrMsg = "Expected format is 'SET', 'SET key', or " +
      "'SET key=value'. If you want to include special characters in key, or include semicolon " +
      "in value, please use quotes, e.g., SET `ke y`=`v;alue`."
    intercept("SET spark.sql.key value", expectedErrMsg)
    intercept("SET spark.sql.key   'value'", expectedErrMsg)
    intercept("SET    spark.sql.key \"value\" ", expectedErrMsg)
    intercept("SET spark.sql.key value1 value2", expectedErrMsg)
    intercept("SET spark.   sql.key=value", expectedErrMsg)
    intercept("SET spark   :sql:key=value", expectedErrMsg)
    intercept("SET spark .  sql.key=value", expectedErrMsg)
    intercept("SET spark.sql.   key=value", expectedErrMsg)
    intercept("SET spark.sql   :key=value", expectedErrMsg)
    intercept("SET spark.sql .  key=value", expectedErrMsg)
    intercept("SET =", expectedErrMsg)
    intercept("SET =value", expectedErrMsg)
  }

  test("Report Error for invalid usage of RESET command") {
    assertEqual("RESET", ResetCommand(None))
    assertEqual("RESET spark.sql.key", ResetCommand(Some("spark.sql.key")))
    assertEqual("RESET  spark.sql.key  ", ResetCommand(Some("spark.sql.key")))
    assertEqual("RESET 1.2.key ", ResetCommand(Some("1.2.key")))
    assertEqual("RESET spark.sql.3", ResetCommand(Some("spark.sql.3")))
    assertEqual("RESET 1:2:key ", ResetCommand(Some("1:2:key")))
    assertEqual("RESET spark:sql:3", ResetCommand(Some("spark:sql:3")))
    assertEqual("RESET `spark.sql.    key`", ResetCommand(Some("spark.sql.    key")))

    val expectedErrMsg = "Expected format is 'RESET' or 'RESET key'. " +
      "If you want to include special characters in key, " +
      "please use quotes, e.g., RESET `ke y`."
    intercept("RESET spark.sql.key1 key2", expectedErrMsg)
    intercept("RESET spark.  sql.key1 key2", expectedErrMsg)
    intercept("RESET spark.sql.key1 key2 key3", expectedErrMsg)
    intercept("RESET spark:   sql:key", expectedErrMsg)
    intercept("RESET spark   .sql.key", expectedErrMsg)
    intercept("RESET spark :  sql:key", expectedErrMsg)
    intercept("RESET spark.sql:   key", expectedErrMsg)
    intercept("RESET spark.sql   .key", expectedErrMsg)
    intercept("RESET spark.sql :  key", expectedErrMsg)
  }

  test("SPARK-33419: Semicolon handling in SET command") {
    assertEqual("SET a=1;", SetCommand(Some("a" -> Some("1"))))
    assertEqual("SET a=1;;", SetCommand(Some("a" -> Some("1"))))

    assertEqual("SET a=`1`;", SetCommand(Some("a" -> Some("1"))))
    assertEqual("SET a=`1;`", SetCommand(Some("a" -> Some("1;"))))
    assertEqual("SET a=`1;`;", SetCommand(Some("a" -> Some("1;"))))

    assertEqual("SET `a`=1;;", SetCommand(Some("a" -> Some("1"))))
    assertEqual("SET `a`=`1;`", SetCommand(Some("a" -> Some("1;"))))
    assertEqual("SET `a`=`1;`;", SetCommand(Some("a" -> Some("1;"))))

    val expectedErrMsg = "Expected format is 'SET', 'SET key', or " +
      "'SET key=value'. If you want to include special characters in key, or include semicolon " +
      "in value, please use quotes, e.g., SET `ke y`=`v;alue`."

    intercept("SET a=1; SELECT 1", expectedErrMsg)
    intercept("SET a=1;2;;", expectedErrMsg)

    intercept("SET a b=`1;;`",
      "'a b' is an invalid property key, please use quotes, e.g. SET `a b`=`1;;`")

    intercept("SET `a`=1;2;;",
      "'1;2;;' is an invalid property value, please use quotes, e.g." +
        " SET `a`=`1;2;;`")
  }

  test("refresh resource") {
    assertEqual("REFRESH prefix_path", RefreshResource("prefix_path"))
    assertEqual("REFRESH /", RefreshResource("/"))
    assertEqual("REFRESH /path///a", RefreshResource("/path///a"))
    assertEqual("REFRESH pat1h/112/_1a", RefreshResource("pat1h/112/_1a"))
    assertEqual("REFRESH pat1h/112/_1a/a-1", RefreshResource("pat1h/112/_1a/a-1"))
    assertEqual("REFRESH path-with-dash", RefreshResource("path-with-dash"))
    assertEqual("REFRESH \'path with space\'", RefreshResource("path with space"))
    assertEqual("REFRESH \"path with space 2\"", RefreshResource("path with space 2"))
    intercept("REFRESH a b", "REFRESH statements cannot contain")
    intercept("REFRESH a\tb", "REFRESH statements cannot contain")
    intercept("REFRESH a\nb", "REFRESH statements cannot contain")
    intercept("REFRESH a\rb", "REFRESH statements cannot contain")
    intercept("REFRESH a\r\nb", "REFRESH statements cannot contain")
    intercept("REFRESH @ $a$", "REFRESH statements cannot contain")
    intercept("REFRESH  ", "Resource paths cannot be empty in REFRESH statements")
    intercept("REFRESH", "Resource paths cannot be empty in REFRESH statements")
  }

  test("SPARK-33118 CREATE TEMPORARY TABLE with LOCATION") {
    assertEqual("CREATE TEMPORARY TABLE t USING parquet OPTIONS (path '/data/tmp/testspark1')",
      CreateTempViewUsing(TableIdentifier("t", None), None, false, false, "parquet",
        Map("path" -> "/data/tmp/testspark1")))
    assertEqual("CREATE TEMPORARY TABLE t USING parquet LOCATION '/data/tmp/testspark1'",
      CreateTempViewUsing(TableIdentifier("t", None), None, false, false, "parquet",
        Map("path" -> "/data/tmp/testspark1")))
  }

  test("describe query") {
    val query = "SELECT * FROM t"
    assertEqual("DESCRIBE QUERY " + query, DescribeQueryCommand(query, parser.parsePlan(query)))
    assertEqual("DESCRIBE " + query, DescribeQueryCommand(query, parser.parsePlan(query)))
  }

  test("query organization") {
    // Test all valid combinations of order by/sort by/distribute by/cluster by/limit/windows
    val baseSql = "select * from t"
    val basePlan =
      Project(Seq(UnresolvedStar(None)), UnresolvedRelation(TableIdentifier("t")))

    assertEqual(s"$baseSql distribute by a, b",
      RepartitionByExpression(UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: Nil,
        basePlan,
        None))
    assertEqual(s"$baseSql distribute by a sort by b",
      Sort(SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
        global = false,
        RepartitionByExpression(UnresolvedAttribute("a") :: Nil,
          basePlan,
          None)))
    assertEqual(s"$baseSql cluster by a, b",
      Sort(SortOrder(UnresolvedAttribute("a"), Ascending) ::
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
        global = false,
        RepartitionByExpression(UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: Nil,
          basePlan,
          None)))
  }

  test("pipeline concatenation") {
    val concat = Concat(
      Concat(UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: Nil) ::
      UnresolvedAttribute("c") ::
      Nil
    )
    assertEqual(
      "SELECT a || b || c FROM t",
      Project(UnresolvedAlias(concat) :: Nil, UnresolvedRelation(TableIdentifier("t"))))
  }

  test("database and schema tokens are interchangeable") {
    assertEqual("CREATE DATABASE foo", parser.parsePlan("CREATE SCHEMA foo"))
    assertEqual("DROP DATABASE foo", parser.parsePlan("DROP SCHEMA foo"))
    assertEqual("ALTER DATABASE foo SET DBPROPERTIES ('x' = 'y')",
      parser.parsePlan("ALTER SCHEMA foo SET DBPROPERTIES ('x' = 'y')"))
    assertEqual("DESC DATABASE foo", parser.parsePlan("DESC SCHEMA foo"))
  }

  test("manage resources") {
    assertEqual("ADD FILE abc.txt", AddFileCommand("abc.txt"))
    assertEqual("ADD FILE 'abc.txt'", AddFileCommand("abc.txt"))
    assertEqual("ADD FILE \"/path/to/abc.txt\"", AddFileCommand("/path/to/abc.txt"))
    assertEqual("LIST FILE abc.txt", ListFilesCommand(Array("abc.txt")))
    assertEqual("LIST FILE '/path//abc.txt'", ListFilesCommand(Array("/path//abc.txt")))
    assertEqual("LIST FILE \"/path2/abc.txt\"", ListFilesCommand(Array("/path2/abc.txt")))
    assertEqual("ADD JAR /path2/_2/abc.jar", AddJarCommand("/path2/_2/abc.jar"))
    assertEqual("ADD JAR '/test/path_2/jar/abc.jar'", AddJarCommand("/test/path_2/jar/abc.jar"))
    assertEqual("ADD JAR \"abc.jar\"", AddJarCommand("abc.jar"))
    assertEqual("LIST JAR /path-with-dash/abc.jar",
      ListJarsCommand(Array("/path-with-dash/abc.jar")))
    assertEqual("LIST JAR 'abc.jar'", ListJarsCommand(Array("abc.jar")))
    assertEqual("LIST JAR \"abc.jar\"", ListJarsCommand(Array("abc.jar")))
    assertEqual("ADD FILE /path with space/abc.txt", AddFileCommand("/path with space/abc.txt"))
    assertEqual("ADD JAR /path with space/abc.jar", AddJarCommand("/path with space/abc.jar"))
  }

  test("SPARK-32608: script transform with row format delimit") {
    assertEqual(
      """
        |SELECT TRANSFORM(a, b, c)
        |  ROW FORMAT DELIMITED
        |  FIELDS TERMINATED BY ','
        |  COLLECTION ITEMS TERMINATED BY '#'
        |  MAP KEYS TERMINATED BY '@'
        |  LINES TERMINATED BY '\n'
        |  NULL DEFINED AS 'null'
        |  USING 'cat' AS (a, b, c)
        |  ROW FORMAT DELIMITED
        |  FIELDS TERMINATED BY ','
        |  COLLECTION ITEMS TERMINATED BY '#'
        |  MAP KEYS TERMINATED BY '@'
        |  LINES TERMINATED BY '\n'
        |  NULL DEFINED AS 'NULL'
        |FROM testData
      """.stripMargin,
    ScriptTransformation(
      Seq('a, 'b, 'c),
      "cat",
      Seq(AttributeReference("a", StringType)(),
        AttributeReference("b", StringType)(),
        AttributeReference("c", StringType)()),
      UnresolvedRelation(TableIdentifier("testData")),
      ScriptInputOutputSchema(
        Seq(("TOK_TABLEROWFORMATFIELD", ","),
          ("TOK_TABLEROWFORMATCOLLITEMS", "#"),
          ("TOK_TABLEROWFORMATMAPKEYS", "@"),
          ("TOK_TABLEROWFORMATNULL", "null"),
          ("TOK_TABLEROWFORMATLINES", "\n")),
        Seq(("TOK_TABLEROWFORMATFIELD", ","),
          ("TOK_TABLEROWFORMATCOLLITEMS", "#"),
          ("TOK_TABLEROWFORMATMAPKEYS", "@"),
          ("TOK_TABLEROWFORMATNULL", "NULL"),
          ("TOK_TABLEROWFORMATLINES", "\n")), None, None,
        List.empty, List.empty, None, None, false)))
  }

  test("SPARK-32607: Script Transformation ROW FORMAT DELIMITED" +
    " `TOK_TABLEROWFORMATLINES` only support '\\n'") {

      // test input format TOK_TABLEROWFORMATLINES
      intercept(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  ROW FORMAT DELIMITED
             |  FIELDS TERMINATED BY ','
             |  LINES TERMINATED BY '@'
             |  NULL DEFINED AS 'null'
             |  USING 'cat' AS (value)
             |  ROW FORMAT DELIMITED
             |  FIELDS TERMINATED BY '&'
             |  LINES TERMINATED BY '\n'
             |  NULL DEFINED AS 'NULL'
             |FROM v
        """.stripMargin,
      "LINES TERMINATED BY only supports newline '\\n' right now")

    // test output format TOK_TABLEROWFORMATLINES
    intercept(
      s"""
         |SELECT TRANSFORM(a, b, c, d, e)
         |  ROW FORMAT DELIMITED
         |  FIELDS TERMINATED BY ','
         |  LINES TERMINATED BY '\n'
         |  NULL DEFINED AS 'null'
         |  USING 'cat' AS (value)
         |  ROW FORMAT DELIMITED
         |  FIELDS TERMINATED BY '&'
         |  LINES TERMINATED BY '@'
         |  NULL DEFINED AS 'NULL'
         |FROM v
        """.stripMargin,
      "LINES TERMINATED BY only supports newline '\\n' right now")
  }

  test("CACHE TABLE") {
    assertEqual(
      "CACHE TABLE a.b.c",
      CacheTableCommand(Seq("a", "b", "c"), None, None, false, Map.empty))

    assertEqual(
      "CACHE TABLE t AS SELECT * FROM testData",
      CacheTableCommand(
        Seq("t"),
        Some(Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("testData")))),
        Some("SELECT * FROM testData"),
        false,
        Map.empty))

    assertEqual(
      "CACHE LAZY TABLE a.b.c",
      CacheTableCommand(Seq("a", "b", "c"), None, None, true, Map.empty))

    assertEqual(
      "CACHE LAZY TABLE a.b.c OPTIONS('storageLevel' 'DISK_ONLY')",
      CacheTableCommand(
        Seq("a", "b", "c"),
        None,
        None,
        true,
        Map("storageLevel" -> "DISK_ONLY")))

    intercept("CACHE TABLE a.b.c AS SELECT * FROM testData",
      "It is not allowed to add catalog/namespace prefix a.b")
  }

  test("UNCACHE TABLE") {
    assertEqual(
      "UNCACHE TABLE a.b.c",
      UncacheTableCommand(Seq("a", "b", "c"), ifExists = false))

    assertEqual(
      "UNCACHE TABLE IF EXISTS a.b.c",
      UncacheTableCommand(Seq("a", "b", "c"), ifExists = true))
  }

  test("CLEAR CACHE") {
    assertEqual("CLEAR CACHE", ClearCacheCommand)
  }
}
