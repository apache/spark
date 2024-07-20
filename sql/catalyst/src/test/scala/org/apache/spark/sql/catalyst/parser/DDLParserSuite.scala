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

package org.apache.spark.sql.catalyst.parser

import java.util.Locale

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Hex, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition.{after, first}
import org.apache.spark.sql.connector.expressions.{ApplyTransform, BucketTransform, ClusterByTransform, DaysTransform, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, Transform, YearsTransform}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.bucket
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.storage.StorageLevelMapper
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class DDLParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  private def parseException(sqlText: String): SparkThrowable = {
    super.parseException(parsePlan)(sqlText)
  }

  private def parseCompare(sql: String, expected: LogicalPlan): Unit = {
    comparePlans(parsePlan(sql), expected, checkAnalysis = false)
  }

  private def internalException(sqlText: String): SparkThrowable = {
    super.internalException(parsePlan)(sqlText)
  }

  test("create/replace table using - schema") {
    val createSql = "CREATE TABLE my_tab(a INT COMMENT 'test', b STRING NOT NULL) USING parquet"
    val replaceSql = "REPLACE TABLE my_tab(a INT COMMENT 'test', b STRING NOT NULL) USING parquet"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(
        ColumnDefinition("a", IntegerType, comment = Some("test")),
        ColumnDefinition("b", StringType, nullable = false)
      )),
      Seq.empty[Transform],
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)

    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }

    val sql = "CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING) USING parquet"
    checkError(
      exception = parseException(sql),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "':'", "hint" -> ""))
  }

  test("create/replace table - with IF NOT EXISTS") {
    val sql = "CREATE TABLE IF NOT EXISTS my_tab(a INT, b STRING) USING parquet"
    testCreateOrReplaceDdl(
      sql,
      TableSpec(
        Seq("my_tab"),
        Some(Seq(ColumnDefinition("a", IntegerType), ColumnDefinition("b", StringType))),
        Seq.empty[Transform],
        Map.empty[String, String],
        Some("parquet"),
        OptionList(Seq.empty),
        None,
        None,
        None),
      expectedIfNotExists = true)
  }

  test("create/replace table - with partitioned by") {
    val createSql = "CREATE TABLE my_tab(a INT comment 'test', b STRING) " +
        "USING parquet PARTITIONED BY (a)"
    val replaceSql = "REPLACE TABLE my_tab(a INT comment 'test', b STRING) " +
      "USING parquet PARTITIONED BY (a)"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(
        ColumnDefinition("a", IntegerType, comment = Some("test")),
        ColumnDefinition("b", StringType))),
      Seq(IdentityTransform(FieldReference("a"))),
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - partitioned by transforms") {
    val createSql =
      """
        |CREATE TABLE my_tab (a INT, b STRING, ts TIMESTAMP) USING parquet
        |PARTITIONED BY (
        |    a,
        |    bucket(16, b),
        |    years(ts),
        |    months(ts),
        |    days(ts),
        |    hours(ts),
        |    foo(a, "bar", 34))
      """.stripMargin

    val replaceSql =
      """
        |REPLACE TABLE my_tab (a INT, b STRING, ts TIMESTAMP) USING parquet
        |PARTITIONED BY (
        |    a,
        |    bucket(16, b),
        |    years(ts),
        |    months(ts),
        |    days(ts),
        |    hours(ts),
        |    foo(a, "bar", 34))
      """.stripMargin
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(
        ColumnDefinition("a", IntegerType),
        ColumnDefinition("b", StringType),
        ColumnDefinition("ts", TimestampType)
      )),
      Seq(
        IdentityTransform(FieldReference("a")),
        BucketTransform(LiteralValue(16, IntegerType), Seq(FieldReference("b"))),
        YearsTransform(FieldReference("ts")),
        MonthsTransform(FieldReference("ts")),
        DaysTransform(FieldReference("ts")),
        HoursTransform(FieldReference("ts")),
        ApplyTransform("foo", Seq(
          FieldReference("a"),
          LiteralValue(UTF8String.fromString("bar"), StringType),
          LiteralValue(34, IntegerType)))),
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with bucket") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
        "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet " +
      "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("a", IntegerType), ColumnDefinition("b", StringType))),
      List(bucket(5, Array(FieldReference.column("a")), Array(FieldReference.column("b")))),
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)

    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with cluster by") {
    // Testing cluster by single part and multipart name.
    Seq(
      ("a INT, b STRING, ts TIMESTAMP",
        "a, b",
        Seq(
          ColumnDefinition("a", IntegerType),
          ColumnDefinition("b", StringType),
          ColumnDefinition("ts", TimestampType)
        ),
        ClusterByTransform(Seq(FieldReference("a"), FieldReference("b")))),
      ("a STRUCT<b INT, c STRING>, ts TIMESTAMP",
        "a.b, ts",
        Seq(
          ColumnDefinition(
            "a", new StructType().add("b", IntegerType).add("c", StringType)),
          ColumnDefinition("ts", TimestampType)
        ),
        ClusterByTransform(Seq(FieldReference(Seq("a", "b")), FieldReference("ts"))))
    ).foreach { case (columns, clusteringColumns, schema, clusterByTransform) =>
      val createSql =
        s"""CREATE TABLE my_tab ($columns) USING parquet
           |CLUSTER BY ($clusteringColumns)
           |""".stripMargin
      val replaceSql =
        s"""REPLACE TABLE my_tab ($columns) USING parquet
           |CLUSTER BY ($clusteringColumns)
           |""".stripMargin
      val expectedTableSpec = TableSpec(
        Seq("my_tab"),
        Some(schema),
        Seq(clusterByTransform),
        Map.empty[String, String],
        Some("parquet"),
        OptionList(Seq.empty),
        None,
        None,
        None)
      Seq(createSql, replaceSql).foreach { sql =>
        testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
      }
    }
  }

  test("create/replace table - with comment") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("a", IntegerType), ColumnDefinition("b", StringType))),
      Seq.empty[Transform],
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      Some("abc"),
      None)
    Seq(createSql, replaceSql).foreach{ sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with table properties") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet" +
      " TBLPROPERTIES('test' = 'test')"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet" +
      " TBLPROPERTIES('test' = 'test')"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("a", IntegerType), ColumnDefinition("b", StringType))),
      Seq.empty[Transform],
      Map("test" -> "test"),
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with location") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("a", IntegerType), ColumnDefinition("b", StringType))),
      Seq.empty[Transform],
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      Some("/tmp/file"),
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - byte length literal table name") {
    val createSql = "CREATE TABLE 1m.2g(a INT) USING parquet"
    val replaceSql = "REPLACE TABLE 1m.2g(a INT) USING parquet"
    val expectedTableSpec = TableSpec(
      Seq("1m", "2g"),
      Some(Seq(ColumnDefinition("a", IntegerType))),
      Seq.empty[Transform],
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - partition column definitions") {
    val createSql = "CREATE TABLE my_tab (id bigint) PARTITIONED BY (part string)"
    val replaceSql = "REPLACE TABLE my_tab (id bigint) PARTITIONED BY (part string)"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - empty columns list") {
    val createSql = "CREATE TABLE my_tab PARTITIONED BY (part string)"
    val replaceSql = "REPLACE TABLE my_tab PARTITIONED BY (part string)"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - using with partition column definitions") {
    val createSql = "CREATE TABLE my_tab (id bigint) USING parquet PARTITIONED BY (part string)"
    val replaceSql = "REPLACE TABLE my_tab (id bigint) USING parquet PARTITIONED BY (part string)"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      Some("parquet"),
      OptionList(Seq.empty),
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - mixed partition references and column definitions") {
    val createSql = "CREATE TABLE my_tab (id bigint, p1 string) PARTITIONED BY (p1, p2 string)"
    val value1 =
      """PARTITION BY: Cannot mix partition expressions and partition columns:
        |Expressions: p1
        |Columns: p2 string""".stripMargin
    checkError(
      exception = parseException(createSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value1),
      context = ExpectedContext(
        fragment = createSql,
        start = 0,
        stop = 72))

    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value1),
      context = ExpectedContext(
        fragment = replaceSql,
        start = 0,
        stop = 73))

    val createSqlWithExpr =
      "CREATE TABLE my_tab (id bigint, p1 string) PARTITIONED BY (p2 string, truncate(p1, 16))"
    val value2 =
      """PARTITION BY: Cannot mix partition expressions and partition columns:
        |Expressions: truncate(p1, 16)
        |Columns: p2 string""".stripMargin
    checkError(
      exception = parseException(createSqlWithExpr),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value2),
      context = ExpectedContext(
        fragment = createSqlWithExpr,
        start = 0,
        stop = 86))

    val replaceSqlWithExpr = createSqlWithExpr.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSqlWithExpr),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value2),
      context = ExpectedContext(
        fragment = replaceSqlWithExpr,
        start = 0,
        stop = 87))
  }

  test("create/replace table - stored as") {
    val createSql =
      """CREATE TABLE my_tab (id bigint)
        |PARTITIONED BY (part string)
        |STORED AS parquet
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      Some(SerdeInfo(storedAs = Some("parquet"))))
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - stored as format with serde") {
    Seq("sequencefile", "textfile", "rcfile").foreach { format =>
      val createSql =
        s"""CREATE TABLE my_tab (id bigint)
          |PARTITIONED BY (part string)
          |STORED AS $format
          |ROW FORMAT SERDE 'customSerde'
          |WITH SERDEPROPERTIES ('prop'='value')
        """.stripMargin
      val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
      val expectedTableSpec = TableSpec(
        Seq("my_tab"),
        Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
        Seq(IdentityTransform(FieldReference("part"))),
        Map.empty[String, String],
        None,
        OptionList(Seq.empty),
        None,
        None,
        Some(SerdeInfo(storedAs = Some(format), serde = Some("customSerde"), serdeProperties = Map(
          "prop" -> "value"
        ))))
      Seq(createSql, replaceSql).foreach { sql =>
        testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
      }
    }

    val createSql =
      s"""CREATE TABLE my_tab (id bigint)
         |PARTITIONED BY (part string)
         |STORED AS otherFormat
         |ROW FORMAT SERDE 'customSerde'
         |WITH SERDEPROPERTIES ('prop'='value')""".stripMargin
    val value = "ROW FORMAT SERDE is incompatible with format 'otherformat', " +
      "which also specifies a serde"
    checkError(
      exception = parseException(createSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value),
      context = ExpectedContext(
        fragment = createSql,
        start = 0,
        stop = 150))

    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value),
      context = ExpectedContext(
        fragment = replaceSql,
        start = 0,
        stop = 151))
  }

  test("create/replace table - stored as format with delimited clauses") {
    val createSql =
      s"""CREATE TABLE my_tab (id bigint)
         |PARTITIONED BY (part string)
         |STORED AS textfile
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' -- double escape for Scala and for SQL
         |COLLECTION ITEMS TERMINATED BY '#'
         |MAP KEYS TERMINATED BY '='
         |LINES TERMINATED BY '\\n'
      """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      Some(SerdeInfo(storedAs = Some("textfile"), serdeProperties = Map(
        "field.delim" -> ",", "serialization.format" -> ",", "escape.delim" -> "\\",
        "colelction.delim" -> "#", "mapkey.delim" -> "=", "line.delim" -> "\n"
      ))))
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }

    val createFailSql =
      s"""CREATE TABLE my_tab (id bigint)
         |PARTITIONED BY (part string)
         |STORED AS otherFormat
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ','""".stripMargin
    val value = "ROW FORMAT DELIMITED is only compatible with 'textfile', not 'otherformat'"
    checkError(
      exception = parseException(createFailSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value),
      context = ExpectedContext(
        fragment = createFailSql,
        start = 0,
        stop = 127))

    val replaceFailSql = createFailSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceFailSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> value),
      context = ExpectedContext(
        fragment = replaceFailSql,
        start = 0,
        stop = 128))
  }

  test("create/replace table - stored as inputformat/outputformat") {
    val createSql =
      """CREATE TABLE my_tab (id bigint)
        |PARTITIONED BY (part string)
        |STORED AS INPUTFORMAT 'inFormat' OUTPUTFORMAT 'outFormat'
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      Some(SerdeInfo(formatClasses = Some(FormatClasses("inFormat", "outFormat")))))
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - stored as inputformat/outputformat with serde") {
    val createSql =
      """CREATE TABLE my_tab (id bigint)
        |PARTITIONED BY (part string)
        |STORED AS INPUTFORMAT 'inFormat' OUTPUTFORMAT 'outFormat'
        |ROW FORMAT SERDE 'customSerde'
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(Seq(ColumnDefinition("id", LongType), ColumnDefinition("part", StringType))),
      Seq(IdentityTransform(FieldReference("part"))),
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      Some(SerdeInfo(
        formatClasses = Some(FormatClasses("inFormat", "outFormat")),
        serde = Some("customSerde"))))
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - using with stored as") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, part string)
        |USING parquet
        |STORED AS parquet""".stripMargin
    checkError(
      exception = parseException(createSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "CREATE TABLE ... USING ... STORED AS PARQUET "),
      context = ExpectedContext(
        fragment = createSql,
        start = 0,
        stop = 75))

    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "REPLACE TABLE ... USING ... STORED AS PARQUET "),
      context = ExpectedContext(
        fragment = replaceSql,
        start = 0,
        stop = 76))
  }

  test("create/replace table - using with row format serde") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, part string)
        |USING parquet
        |ROW FORMAT SERDE 'customSerde'""".stripMargin
    checkError(
      exception = parseException(createSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "CREATE TABLE ... USING ... ROW FORMAT SERDE CUSTOMSERDE"),
      context = ExpectedContext(
        fragment = createSql,
        start = 0,
        stop = 88))

    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "REPLACE TABLE ... USING ... ROW FORMAT SERDE CUSTOMSERDE"),
      context = ExpectedContext(
        fragment = replaceSql,
        start = 0,
        stop = 89))
  }

  test("create/replace table - using with row format delimited") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, part string)
        |USING parquet
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""".stripMargin
    checkError(
      exception = parseException(createSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "CREATE TABLE ... USING ... ROW FORMAT DELIMITED"),
      context = ExpectedContext(
        fragment = createSql,
        start = 0,
        stop = 103))

    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "REPLACE TABLE ... USING ... ROW FORMAT DELIMITED"),
      context = ExpectedContext(
        fragment = replaceSql,
        start = 0,
        stop = 104))
  }

  test("create/replace table - stored by") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, p1 string)
        |STORED BY 'handler'""".stripMargin
    val fragment = "STORED BY 'handler'"
    checkError(
      exception = parseException(createSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "STORED BY"),
      context = ExpectedContext(
        fragment = fragment,
        start = 43,
        stop = 61))

    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    checkError(
      exception = parseException(replaceSql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "STORED BY"),
      context = ExpectedContext(
        fragment = fragment,
        start = 44,
        stop = 62))
  }

  test("Unsupported skew clause - create/replace table") {
    val sql1 = "CREATE TABLE my_tab (id bigint) SKEWED BY (id) ON (1,2,3)"
    checkError(
      exception = parseException(sql1),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "CREATE TABLE ... SKEWED BY"),
      context = ExpectedContext(
        fragment = sql1,
        start = 0,
        stop = 56))

    val sql2 = "REPLACE TABLE my_tab (id bigint) SKEWED BY (id) ON (1,2,3)"
    checkError(
      exception = parseException(sql2),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "CREATE TABLE ... SKEWED BY"),
      context = ExpectedContext(
        fragment = sql2,
        start = 0,
        stop = 57))
  }

  test("Duplicate clauses - create/replace table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) $duplicateClause $duplicateClause"
    }

    def replaceTableHeader(duplicateClause: String): String = {
      s"REPLACE TABLE my_tab(a INT, b STRING) $duplicateClause $duplicateClause"
    }

    val sql1 = createTableHeader("TBLPROPERTIES('test' = 'test2')")
    checkError(
      exception = parseException(sql1),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "TBLPROPERTIES"),
      context = ExpectedContext(
        fragment = sql1,
        start = 0,
        stop = 99))

    val sql2 = createTableHeader("LOCATION '/tmp/file'")
    checkError(
      exception = parseException(sql2),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "LOCATION"),
      context = ExpectedContext(
        fragment = sql2,
        start = 0,
        stop = 77))

    val sql3 = createTableHeader("COMMENT 'a table'")
    checkError(
      exception = parseException(sql3),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "COMMENT"),
      context = ExpectedContext(
        fragment = sql3,
        start = 0,
        stop = 71))

    val sql4 = createTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS")
    checkError(
      exception = parseException(sql4),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "CLUSTERED BY"),
      context = ExpectedContext(
        fragment = sql4,
        start = 0,
        stop = 101))

    val sql5 = createTableHeader("PARTITIONED BY (b)")
    checkError(
      exception = parseException(sql5),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "PARTITIONED BY"),
      context = ExpectedContext(
        fragment = sql5,
        start = 0,
        stop = 73))

    val sql6 = createTableHeader("PARTITIONED BY (c int)")
    checkError(
      exception = parseException(sql6),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "PARTITIONED BY"),
      context = ExpectedContext(
        fragment = sql6,
        start = 0,
        stop = 81))

    val sql7 = createTableHeader("STORED AS parquet")
    checkError(
      exception = parseException(sql7),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "STORED AS/BY"),
      context = ExpectedContext(
        fragment = sql7,
        start = 0,
        stop = 71))

    val sql8 = createTableHeader("STORED AS INPUTFORMAT 'in' OUTPUTFORMAT 'out'")
    checkError(
      exception = parseException(sql8),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "STORED AS/BY"),
      context = ExpectedContext(
        fragment = sql8,
        start = 0,
        stop = 127))

    val sql9 = createTableHeader("ROW FORMAT SERDE 'serde'")
    checkError(
      exception = parseException(sql9),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "ROW FORMAT"),
      context = ExpectedContext(
        fragment = sql9,
        start = 0,
        stop = 85))

    val sql10 = replaceTableHeader("TBLPROPERTIES('test' = 'test2')")
    checkError(
      exception = parseException(sql10),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "TBLPROPERTIES"),
      context = ExpectedContext(
        fragment = sql10,
        start = 0,
        stop = 100))

    val sql11 = replaceTableHeader("LOCATION '/tmp/file'")
    checkError(
      exception = parseException(sql11),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "LOCATION"),
      context = ExpectedContext(
        fragment = sql11,
        start = 0,
        stop = 78))

    val sql12 = replaceTableHeader("COMMENT 'a table'")
    checkError(
      exception = parseException(sql12),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "COMMENT"),
      context = ExpectedContext(
        fragment = sql12,
        start = 0,
        stop = 72))

    val sql13 = replaceTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS")
    checkError(
      exception = parseException(sql13),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "CLUSTERED BY"),
      context = ExpectedContext(
        fragment = sql13,
        start = 0,
        stop = 102))

    val sql14 = replaceTableHeader("PARTITIONED BY (b)")
    checkError(
      exception = parseException(sql14),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "PARTITIONED BY"),
      context = ExpectedContext(
        fragment = sql14,
        start = 0,
        stop = 74))

    val sql15 = replaceTableHeader("PARTITIONED BY (c int)")
    checkError(
      exception = parseException(sql15),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "PARTITIONED BY"),
      context = ExpectedContext(
        fragment = sql15,
        start = 0,
        stop = 82))

    val sql16 = replaceTableHeader("STORED AS parquet")
    checkError(
      exception = parseException(sql16),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "STORED AS/BY"),
      context = ExpectedContext(
        fragment = sql16,
        start = 0,
        stop = 72))

    val sql17 = replaceTableHeader("STORED AS INPUTFORMAT 'in' OUTPUTFORMAT 'out'")
    checkError(
      exception = parseException(sql17),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "STORED AS/BY"),
      context = ExpectedContext(
        fragment = sql17,
        start = 0,
        stop = 128))

    val sql18 = replaceTableHeader("ROW FORMAT SERDE 'serde'")
    checkError(
      exception = parseException(sql18),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "ROW FORMAT"),
      context = ExpectedContext(
        fragment = sql18,
        start = 0,
        stop = 86))

    val sql19 = createTableHeader("CLUSTER BY (a)")
    checkError(
      exception = parseException(sql19),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "CLUSTER BY"),
      context = ExpectedContext(
        fragment = sql19,
        start = 0,
        stop = 65))

    val sql20 = replaceTableHeader("CLUSTER BY (a)")
    checkError(
      exception = parseException(sql20),
      errorClass = "DUPLICATE_CLAUSES",
      parameters = Map("clauseName" -> "CLUSTER BY"),
      context = ExpectedContext(
        fragment = sql20,
        start = 0,
        stop = 66))
  }

  test("support for other types in OPTIONS") {
    val createSql =
      """
        |CREATE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin
    val replaceSql =
      """
        |REPLACE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(
        sql,
        TableSpec(
          Seq("table_name"),
          Some(Nil),
          Seq.empty[Transform],
          Map.empty,
          Some("json"),
          OptionList(
            Seq(
              ("a", Literal(1)),
              ("b", Literal(Decimal(0.1))),
              ("c" -> Literal(true)))),
          None,
          None,
          None),
        expectedIfNotExists = false)
    }
  }

  test("Test CTAS against native tables") {
    val s1 =
      """
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s2 =
      """
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING parquet
        |LOCATION '/user/external/page_view'
        |COMMENT 'This is the staging page view table'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s3 =
      """
        |CREATE TABLE IF NOT EXISTS mydb.page_view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val s4 =
      """
        |REPLACE TABLE mydb.page_view
        |USING parquet
        |COMMENT 'This is the staging page view table'
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src
      """.stripMargin

    val expectedTableSpec = TableSpec(
        Seq("mydb", "page_view"),
        None,
        Seq.empty[Transform],
        Map("p1" -> "v1", "p2" -> "v2"),
        Some("parquet"),
        OptionList(Seq.empty),
        Some("/user/external/page_view"),
        Some("This is the staging page view table"),
        None)
    Seq(s1, s2, s3, s4).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = true)
    }
  }

  test("drop view") {
    val cmd = "DROP VIEW"
    val hint = Some("Please use DROP TABLE instead.")
    parseCompare(s"DROP VIEW testcat.db.view",
      DropView(UnresolvedIdentifier(Seq("testcat", "db", "view"), true), ifExists = false))
    parseCompare(s"DROP VIEW db.view",
      DropView(UnresolvedIdentifier(Seq("db", "view"), true), ifExists = false))
    parseCompare(s"DROP VIEW IF EXISTS db.view",
      DropView(UnresolvedIdentifier(Seq("db", "view"), true), ifExists = true))
    parseCompare(s"DROP VIEW view",
      DropView(UnresolvedIdentifier(Seq("view"), true), ifExists = false))
    parseCompare(s"DROP VIEW IF EXISTS view",
      DropView(UnresolvedIdentifier(Seq("view"), true), ifExists = true))
  }

  private def testCreateOrReplaceDdl(
      sqlStatement: String,
      tableSpec: TableSpec,
      expectedIfNotExists: Boolean): Unit = {
    val parsedPlan = parsePlan(sqlStatement)
    val newTableToken = sqlStatement.split(" ")(0).trim.toUpperCase(Locale.ROOT)
    parsedPlan match {
      case create: CreateTable if newTableToken == "CREATE" =>
        assert(create.ignoreIfExists == expectedIfNotExists)
      case ctas: CreateTableAsSelect if newTableToken == "CREATE" =>
        assert(ctas.ignoreIfExists == expectedIfNotExists)
      case replace: ReplaceTable if newTableToken == "REPLACE" =>
      case replace: ReplaceTableAsSelect if newTableToken == "REPLACE" =>
      case other =>
        fail("First token in statement does not match the expected parsed plan; CREATE TABLE" +
          " should create a CreateTableStatement, and REPLACE TABLE should create a" +
          s" ReplaceTableStatement. Statement: $sqlStatement, plan type:" +
          s" ${parsedPlan.getClass.getName}.")
    }
    assert(TableSpec(parsedPlan) === tableSpec)
  }

  // ALTER VIEW view_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter view: alter view properties") {
    val sql1_view = "ALTER VIEW table_name SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_view = "ALTER VIEW table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_view = "ALTER VIEW table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    comparePlans(parsePlan(sql1_view),
      SetViewProperties(
        UnresolvedView(Seq("table_name"), "ALTER VIEW ... SET TBLPROPERTIES", false, true),
        Map("test" -> "test", "comment" -> "new_comment")))
    comparePlans(parsePlan(sql2_view),
      UnsetViewProperties(
        UnresolvedView(Seq("table_name"), "ALTER VIEW ... UNSET TBLPROPERTIES", false, true),
        Seq("comment", "test"),
        ifExists = false))
    comparePlans(parsePlan(sql3_view),
      UnsetViewProperties(
        UnresolvedView(Seq("table_name"), "ALTER VIEW ... UNSET TBLPROPERTIES", false, true),
        Seq("comment", "test"),
        ifExists = true))
  }

  test("alter table: add column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None, None)
      )))
  }

  test("alter table: add multiple columns") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS x int, y string"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS"),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None, None),
          QualifiedColType(None, "y", StringType, true, None, None, None)
      )))
  }

  test("alter table: add column with COLUMNS") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS x int"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS"),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None, None)
      )))
  }

  test("alter table: add column with COLUMNS (...)") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS (x int)"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS"),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None, None)
      )))
  }

  test("alter table: add column with COLUMNS (...) and COMMENT") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS (x int COMMENT 'doc')"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS"),
        Seq(QualifiedColType(None, "x", IntegerType, true, Some("doc"), None, None)
      )))
  }

  test("alter table: add non-nullable column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int NOT NULL"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(None, "x", IntegerType, false, None, None, None)
      )))
  }

  test("alter table: add column with COMMENT") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int COMMENT 'doc'"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(None, "x", IntegerType, true, Some("doc"), None, None)
      )))
  }

  test("alter table: add column with position") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int FIRST"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(
          None,
          "x",
          IntegerType,
          true,
          None,
          Some(UnresolvedFieldPosition(first())),
          None)
      )))

    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int AFTER y"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(
          None,
          "x",
          IntegerType,
          true,
          None,
          Some(UnresolvedFieldPosition(after("y"))),
          None)
      )))
  }

  test("alter table: add column with nested column name") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x.y.z int COMMENT 'doc'"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(
          Some(UnresolvedFieldName(Seq("x", "y"))), "z", IntegerType, true, Some("doc"), None, None)
      )))
  }

  test("alter table: add multiple columns with nested column name") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x.y.z int COMMENT 'doc', a.b string FIRST"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN"),
        Seq(
          QualifiedColType(
            Some(UnresolvedFieldName(Seq("x", "y"))),
            "z",
            IntegerType,
            true,
            Some("doc"),
            None,
            None),
          QualifiedColType(
            Some(UnresolvedFieldName(Seq("a"))),
            "b",
            StringType,
            true,
            None,
            Some(UnresolvedFieldPosition(first())),
            None)
      )))
  }

  test("alter table: update column type using ALTER") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bigint"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ALTER COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(LongType),
        None,
        None,
        None,
        None))
  }

  test("alter table: update column type invalid type") {
    val sql = "ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bad_type"
    val fragment = "bad_type"
    checkError(
      exception = parseException(sql),
      errorClass = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"BAD_TYPE\""),
      context = ExpectedContext(
        fragment = fragment,
        start = 47,
        stop = 54))
  }

  test("alter table: update column type") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c TYPE bigint"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(LongType),
        None,
        None,
        None,
        None))
  }

  test("alter table: update column comment") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c COMMENT 'new comment'"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        Some("new comment"),
        None,
        None))
  }

  test("alter table: update column position") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c FIRST"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        None,
        Some(UnresolvedFieldPosition(first())),
        None))
  }

  test("alter table: multiple property changes are not allowed") {
    val sql1 = "ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bigint COMMENT 'new comment'"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'COMMENT'", "hint" -> ""))

    val sql2 = "ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bigint COMMENT AFTER d"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'COMMENT'", "hint" -> ""))

    val sql3 = "ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bigint COMMENT 'new comment' AFTER d"
    checkError(
      exception = parseException(sql3),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'COMMENT'", "hint" -> ""))
  }

  test("alter table: SET/DROP NOT NULL") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c SET NOT NULL"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ALTER COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        Some(false),
        None,
        None,
        None))

    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c DROP NOT NULL"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ALTER COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        Some(true),
        None,
        None,
        None))
  }

  test("alter table: hive style change column") {
    val sql1 = "ALTER TABLE table_name CHANGE COLUMN a.b.c c INT"
    val sql2 = "ALTER TABLE table_name CHANGE COLUMN a.b.c c INT COMMENT 'new_comment'"
    val sql3 = "ALTER TABLE table_name CHANGE COLUMN a.b.c c INT AFTER other_col"

    comparePlans(
      parsePlan(sql1),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(IntegerType),
        None,
        None,
        None,
        None))

    comparePlans(
      parsePlan(sql2),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(IntegerType),
        None,
        Some("new_comment"),
        None,
        None))

    comparePlans(
      parsePlan(sql3),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(IntegerType),
        None,
        None,
        Some(UnresolvedFieldPosition(after("other_col"))),
        None))

    // renaming column not supported in hive style ALTER COLUMN.
    val sql4 = "ALTER TABLE table_name CHANGE COLUMN a.b.c new_name INT"
    checkError(
      exception = parseException(sql4),
      errorClass = "_LEGACY_ERROR_TEMP_0034",
      parameters = Map(
        "operation" -> "Renaming column",
        "command" -> "ALTER COLUMN",
        "msg" -> ", please run RENAME COLUMN instead"),
      context = ExpectedContext(
        fragment = sql4,
        start = 0,
        stop = 54))

    // ALTER COLUMN for a partition is not supported.
    val sql5 = "ALTER TABLE table_name PARTITION (a='1') CHANGE COLUMN a.b.c c INT"
    checkError(
      exception = parseException(sql5),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE ... PARTITION ... CHANGE COLUMN"),
      context = ExpectedContext(
        fragment = sql5,
        start = 0,
        stop = 65))
  }

  test("alter table: hive style replace columns") {
    val sql1 = "ALTER TABLE table_name REPLACE COLUMNS (x string)"
    val sql2 = "ALTER TABLE table_name REPLACE COLUMNS (x string COMMENT 'x1')"
    val sql3 = "ALTER TABLE table_name REPLACE COLUMNS (x string COMMENT 'x1', y int)"
    val sql4 = "ALTER TABLE table_name REPLACE COLUMNS (x string COMMENT 'x1', y int COMMENT 'y1')"

    comparePlans(
      parsePlan(sql1),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS"),
        Seq(QualifiedColType(None, "x", StringType, true, None, None, None))))

    comparePlans(
      parsePlan(sql2),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS"),
        Seq(QualifiedColType(None, "x", StringType, true, Some("x1"), None, None))))

    comparePlans(
      parsePlan(sql3),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS"),
        Seq(
          QualifiedColType(None, "x", StringType, true, Some("x1"), None, None),
          QualifiedColType(None, "y", IntegerType, true, None, None, None)
        )))

    comparePlans(
      parsePlan(sql4),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS"),
        Seq(
          QualifiedColType(None, "x", StringType, true, Some("x1"), None, None),
          QualifiedColType(None, "y", IntegerType, true, Some("y1"), None, None)
        )))

    val sql5 = "ALTER TABLE table_name PARTITION (a='1') REPLACE COLUMNS (x string)"
    checkError(
      exception = parseException(sql5),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE ... PARTITION ... REPLACE COLUMNS"),
      context = ExpectedContext(
        fragment = sql5,
        start = 0,
        stop = 66))

    val sql6 = "ALTER TABLE table_name REPLACE COLUMNS (x string NOT NULL)"
    checkError(
      exception = parseException(sql6),
      errorClass = "_LEGACY_ERROR_TEMP_0034",
      parameters = Map("operation" -> "NOT NULL", "command" -> "REPLACE COLUMNS", "msg" -> ""),
      context = ExpectedContext(
        fragment = sql6,
        start = 0,
        stop = 57))

    val sql7 = "ALTER TABLE table_name REPLACE COLUMNS (x string FIRST)"
    checkError(
      exception = parseException(sql7),
      errorClass = "_LEGACY_ERROR_TEMP_0034",
      parameters = Map(
        "operation" -> "Column position",
        "command" -> "REPLACE COLUMNS",
        "msg" -> ""),
      context = ExpectedContext(
        fragment = sql7,
        start = 0,
        stop = 54))

    val sql8 = "ALTER TABLE table_name REPLACE COLUMNS (a.b.c string)"
    checkError(
      exception = parseException(sql8),
      errorClass = "_LEGACY_ERROR_TEMP_0034",
      parameters = Map(
        "operation" -> "Replacing with a nested column",
        "command" -> "REPLACE COLUMNS",
        "msg" -> ""),
      context = ExpectedContext(
        fragment = sql8,
        start = 0,
        stop = 52))

    val sql9 = "ALTER TABLE table_name REPLACE COLUMNS (a STRING COMMENT 'x' COMMENT 'y')"
    checkError(
      exception = parseException(sql9),
      errorClass = "ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map(
        "type" -> "REPLACE",
        "columnName" -> "a",
        "optionName" -> "COMMENT"),
      context = ExpectedContext(
        fragment = sql9,
        start = 0,
        stop = 72))
  }

  test("alter view: rename view") {
    comparePlans(
      parsePlan("ALTER VIEW a.b.c RENAME TO x.y.z"),
      RenameTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ALTER VIEW ... RENAME TO", true),
        Seq("x", "y", "z"),
        isView = true))
  }

  test("insert table: basic append") {
    Seq(
      "INSERT INTO TABLE testcat.ns1.ns2.tbl SELECT * FROM source",
      "INSERT INTO testcat.ns1.ns2.tbl SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Nil,
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = false, ifPartitionNotExists = false))
    }
  }

  test("insert table: basic append with a column list") {
    Seq(
      "INSERT INTO TABLE testcat.ns1.ns2.tbl (a, b) SELECT * FROM source",
      "INSERT INTO testcat.ns1.ns2.tbl (a, b) SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Seq("a", "b"),
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = false, ifPartitionNotExists = false))
    }
  }

  test("insert table: append from another catalog") {
    parseCompare("INSERT INTO TABLE testcat.ns1.ns2.tbl SELECT * FROM testcat2.db.tbl",
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map.empty,
        Nil,
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("testcat2", "db", "tbl"))),
        overwrite = false, ifPartitionNotExists = false))
  }

  test("insert table: append with partition") {
    parseCompare(
      """
        |INSERT INTO testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2)
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3"), "p2" -> None),
        Nil,
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = false, ifPartitionNotExists = false))
  }

  test("insert table: append with partition and a column list") {
    parseCompare(
      """
        |INSERT INTO testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2) (a, b)
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3"), "p2" -> None),
        Seq("a", "b"),
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = false, ifPartitionNotExists = false))
  }

  test("insert table: overwrite") {
    Seq(
      "INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl SELECT * FROM source",
      "INSERT OVERWRITE testcat.ns1.ns2.tbl SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Nil,
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = true, ifPartitionNotExists = false))
    }
  }

  test("insert table: overwrite with column list") {
    Seq(
      "INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl (a, b) SELECT * FROM source",
      "INSERT OVERWRITE testcat.ns1.ns2.tbl (a, b) SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Seq("a", "b"),
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = true, ifPartitionNotExists = false))
    }
  }

  test("insert table: overwrite with partition") {
    parseCompare(
      """
        |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2)
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3"), "p2" -> None),
        Nil,
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = true, ifPartitionNotExists = false))
  }

  test("insert table: overwrite with partition and column list") {
    parseCompare(
      """
        |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2) (a, b)
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3"), "p2" -> None),
        Seq("a", "b"),
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = true, ifPartitionNotExists = false))
  }

  test("insert table: overwrite with partition if not exists") {
    parseCompare(
      """
        |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3) IF NOT EXISTS
        |SELECT * FROM source
      """.stripMargin,
      InsertIntoStatement(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Map("p1" -> Some("3")),
        Nil,
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
        overwrite = true, ifPartitionNotExists = true))
  }

  test("insert table: if not exists with dynamic partition fails") {
    val sql =
      """INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2) IF NOT EXISTS
        |SELECT * FROM source""".stripMargin
    val fragment =
      """INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3, p2) IF NOT EXISTS""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "IF NOT EXISTS with dynamic partitions: p2"),
      context = ExpectedContext(
        fragment = fragment,
        start = 0,
        stop = 78))
  }

  test("insert table: if not exists without overwrite fails") {
    val sql =
      """INSERT INTO TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3) IF NOT EXISTS
        |SELECT * FROM source""".stripMargin
    val fragment =
      """INSERT INTO TABLE testcat.ns1.ns2.tbl
        |PARTITION (p1 = 3) IF NOT EXISTS""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "INSERT INTO ... IF NOT EXISTS"),
      context = ExpectedContext(
        fragment = fragment,
        start = 0,
        stop = 69))
  }

  test("insert table: by name") {
    Seq(
      "INSERT INTO TABLE testcat.ns1.ns2.tbl BY NAME SELECT * FROM source",
      "INSERT INTO testcat.ns1.ns2.tbl BY NAME SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Nil,
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = false, ifPartitionNotExists = false, byName = true))
    }

    Seq(
      "INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl BY NAME SELECT * FROM source",
      "INSERT OVERWRITE testcat.ns1.ns2.tbl BY NAME SELECT * FROM source"
    ).foreach { sql =>
      parseCompare(sql,
        InsertIntoStatement(
          UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
          Map.empty,
          Nil,
          Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("source"))),
          overwrite = true, ifPartitionNotExists = false, byName = true))
    }
  }

  test("insert table: by name unsupported case") {
    checkError(
      exception = parseException(
        "INSERT INTO TABLE t1 BY NAME (c1,c2) SELECT * FROM tmp_view"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map(
        "error" -> "'c1'",
        "hint" -> "")
    )
  }

  test("delete from table: delete all") {
    parseCompare("DELETE FROM testcat.ns1.ns2.tbl",
      DeleteFromTable(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Literal.TrueLiteral))
  }

  test("delete from table: with alias and where clause") {
    parseCompare("DELETE FROM testcat.ns1.ns2.tbl AS t WHERE t.a = 2",
      DeleteFromTable(
        SubqueryAlias("t", UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl"))),
        EqualTo(UnresolvedAttribute("t.a"), Literal(2))))
  }

  test("delete from table: column aliases are not allowed") {
    val sql = "DELETE FROM testcat.ns1.ns2.tbl AS t(a,b,c,d) WHERE d = 2"
    checkError(
      exception = parseException(sql),
      errorClass = "COLUMN_ALIASES_NOT_ALLOWED",
      parameters = Map("op" -> "DELETE"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 56))
  }

  test("update table: basic") {
    parseCompare(
      """
        |UPDATE testcat.ns1.ns2.tbl
        |SET a='Robert', b=32
      """.stripMargin,
      UpdateTable(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        Seq(Assignment(UnresolvedAttribute("a"), Literal("Robert")),
          Assignment(UnresolvedAttribute("b"), Literal(32))),
        None))
  }

  test("update table: with alias and where clause") {
    parseCompare(
      """
        |UPDATE testcat.ns1.ns2.tbl AS t
        |SET t.a='Robert', t.b=32
        |WHERE t.c=2
      """.stripMargin,
      UpdateTable(
        SubqueryAlias("t", UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl"))),
        Seq(Assignment(UnresolvedAttribute("t.a"), Literal("Robert")),
          Assignment(UnresolvedAttribute("t.b"), Literal(32))),
        Some(EqualTo(UnresolvedAttribute("t.c"), Literal(2)))))
  }

  test("update table: column aliases are not allowed") {
    val sql =
      """UPDATE testcat.ns1.ns2.tbl AS t(a,b,c,d)
        |SET b='Robert', c=32
        |WHERE d=2""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "COLUMN_ALIASES_NOT_ALLOWED",
      parameters = Map("op" -> "UPDATE"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 70))
  }

  test("merge into table: basic") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='delete') THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='update')
        |THEN UPDATE SET target.col3 = 'delete'
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col3"), Literal("delete"))))),
        withSchemaEvolution = false))
  }

  test("merge into table: using subquery") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING (SELECT * FROM testcat2.ns1.ns2.tbl) AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='delete') THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='update')
        |THEN UPDATE SET target.col3 = 'delete'
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", Project(Seq(UnresolvedStar(None)),
          UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl")))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col3"), Literal("delete"))))),
        withSchemaEvolution = false))
  }

  test("merge into table: cte") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING (WITH s as (SELECT * FROM testcat2.ns1.ns2.tbl) SELECT * FROM s) AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='delete') THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='update')
        |THEN UPDATE SET target.col3 = 'delete'
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", UnresolvedWith(Project(Seq(UnresolvedStar(None)),
          UnresolvedRelation(Seq("s"))),
          Seq("s" -> SubqueryAlias("s", Project(Seq(UnresolvedStar(None)),
            UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))))))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("source.col2"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col3"), Literal("delete"))))),
        withSchemaEvolution = false))
  }

  test("merge into table: no additional condition") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED THEN UPDATE SET target.col2 = source.col2
        |WHEN NOT MATCHED
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        |WHEN NOT MATCHED BY SOURCE THEN DELETE
      """.stripMargin,
    MergeIntoTable(
      SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
      SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
      EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
      Seq(UpdateAction(None,
        Seq(Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
      Seq(InsertAction(None,
        Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
          Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
      Seq(DeleteAction(None)),
      withSchemaEvolution = false))
  }

  test("merge into table: star") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET *
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT *
      """.stripMargin,
    MergeIntoTable(
      SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
      SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
      EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
      Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
        UpdateStarAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))))),
      Seq(InsertStarAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))))),
      Seq.empty,
      withSchemaEvolution = false))
  }

  test("merge into table: invalid star in not matched by source") {
    val sql = """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN NOT MATCHED BY SOURCE THEN UPDATE *
      """.stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'*'", "hint" -> ""))
  }

  test("merge into table: not matched by target") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN NOT MATCHED BY TARGET AND (target.col3='insert1')
        |THEN INSERT (target.col1, target.col2) VALUES (source.col1, 0)
        |WHEN NOT MATCHED AND (target.col3='insert2')
        |THEN INSERT (target.col1, target.col2) VALUES (1, source.col2)
        |WHEN NOT MATCHED BY TARGET
        |THEN INSERT *
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq.empty,
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("insert1"))),
            Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
              Assignment(UnresolvedAttribute("target.col2"), Literal(0)))),
          InsertAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("insert2"))),
            Seq(Assignment(UnresolvedAttribute("target.col1"), Literal(1)),
              Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))),
          InsertStarAction(None)),
        Seq.empty,
        withSchemaEvolution = false))
  }

  test("merge into table: column aliases are not allowed") {
    Seq("target(c1, c2)" -> "source", "target" -> "source(c1, c2)").foreach {
      case (targetAlias, sourceAlias) =>
        val sql = s"""MERGE INTO testcat1.ns1.ns2.tbl AS $targetAlias
             |USING testcat2.ns1.ns2.tbl AS $sourceAlias
             |ON target.col1 = source.col1
             |WHEN MATCHED AND (target.col2='delete') THEN DELETE
             |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
             |WHEN NOT MATCHED AND (target.col2='insert')
             |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)"""
          .stripMargin
        checkError(
          exception = parseException(sql),
          errorClass = "COLUMN_ALIASES_NOT_ALLOWED",
          parameters = Map("op" -> "MERGE"),
          context = ExpectedContext(
            fragment = sql,
            start = 0,
            stop = 365))
    }
  }

  test("merge into table: multi matched, not matched and not matched by source clauses") {
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update1') THEN UPDATE SET target.col2 = 1
        |WHEN MATCHED AND (target.col2='update2') THEN UPDATE SET target.col2 = 2
        |WHEN NOT MATCHED AND (target.col2='insert1')
        |THEN INSERT (target.col1, target.col2) values (source.col1, 1)
        |WHEN NOT MATCHED AND (target.col2='insert2')
        |THEN INSERT (target.col1, target.col2) values (source.col1, 2)
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='delete') THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='update1') THEN UPDATE SET target.col3 = 1
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='update2') THEN UPDATE SET target.col3 = 2
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update1"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"), Literal(1)))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update2"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"), Literal(2))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert1"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), Literal(1)))),
          InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert2"))),
            Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
              Assignment(UnresolvedAttribute("target.col2"), Literal(2))))),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("update1"))),
            Seq(Assignment(UnresolvedAttribute("target.col3"), Literal(1)))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col3"), Literal("update2"))),
            Seq(Assignment(UnresolvedAttribute("target.col3"), Literal(2))))),
        withSchemaEvolution = false))
  }

  test("merge into table: schema evolution") {
    parseCompare(
      """
        |MERGE WITH SCHEMA EVOLUTION INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN NOT MATCHED BY SOURCE THEN DELETE
    """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        matchedActions = Seq.empty,
        notMatchedActions = Seq.empty,
        notMatchedBySourceActions = Seq(DeleteAction(None)),
        withSchemaEvolution = true))
  }

  test("merge into table: only the last matched clause can omit the condition") {
    val sql =
      """MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2 == 'update1') THEN UPDATE SET target.col2 = 1
        |WHEN MATCHED THEN UPDATE SET target.col2 = 2
        |WHEN MATCHED THEN DELETE
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 369))
  }

  test("merge into table: only the last not matched clause can omit the condition") {
    val sql =
      """MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2 == 'update') THEN UPDATE SET target.col2 = source.col2
        |WHEN MATCHED THEN DELETE
        |WHEN NOT MATCHED AND (target.col2='insert1')
        |THEN INSERT (target.col1, target.col2) values (source.col1, 1)
        |WHEN NOT MATCHED
        |THEN INSERT (target.col1, target.col2) values (source.col1, 2)
        |WHEN NOT MATCHED
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 494))
  }

  test("merge into table: only the last not matched by source clause can omit the " +
       "condition") {
    val sql =
      """MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2 == 'update') THEN UPDATE SET target.col2 = source.col2
        |WHEN MATCHED THEN DELETE
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        |WHEN NOT MATCHED BY SOURCE AND (target.col3='update')
        |THEN UPDATE SET target.col3 = 'delete'
        |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET target.col3 = 'update'
        |WHEN NOT MATCHED BY SOURCE THEN DELETE""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 531))
  }

  test("merge into table: there must be a when (not) matched condition") {
    val sql =
      """MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0008",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 106))
  }

  test("show views") {
    comparePlans(
      parsePlan("SHOW VIEWS"),
      ShowViews(CurrentNamespace, None))
    comparePlans(
      parsePlan("SHOW VIEWS '*test*'"),
      ShowViews(CurrentNamespace, Some("*test*")))
    comparePlans(
      parsePlan("SHOW VIEWS LIKE '*test*'"),
      ShowViews(CurrentNamespace, Some("*test*")))
    comparePlans(
      parsePlan("SHOW VIEWS FROM testcat.ns1.ns2.tbl"),
      ShowViews(UnresolvedNamespace(Seq("testcat", "ns1", "ns2", "tbl")), None))
    comparePlans(
      parsePlan("SHOW VIEWS IN testcat.ns1.ns2.tbl"),
      ShowViews(UnresolvedNamespace(Seq("testcat", "ns1", "ns2", "tbl")), None))
    comparePlans(
      parsePlan("SHOW VIEWS IN ns1 '*test*'"),
      ShowViews(UnresolvedNamespace(Seq("ns1")), Some("*test*")))
    comparePlans(
      parsePlan("SHOW VIEWS IN ns1 LIKE '*test*'"),
      ShowViews(UnresolvedNamespace(Seq("ns1")), Some("*test*")))
  }

  test("analyze table statistics") {
    comparePlans(parsePlan("analyze table a.b.c compute statistics"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map.empty, noScan = false))
    comparePlans(parsePlan("analyze table a.b.c compute statistics noscan"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map.empty, noScan = true))
    comparePlans(parsePlan("analyze table a.b.c partition (a) compute statistics nOscAn"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("a" -> None), noScan = true))

    // Partitions specified
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> Some("2008-04-09"), "hr" -> Some("11")), noScan = false))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> Some("2008-04-09"), "hr" -> Some("11")), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09') COMPUTE STATISTICS noscan"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> Some("2008-04-09")), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> Some("2008-04-09"), "hr" -> None), noScan = false))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS noscan"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> Some("2008-04-09"), "hr" -> None), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds, hr=11) COMPUTE STATISTICS noscan"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> None, "hr" -> Some("11")), noScan = true))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds, hr) COMPUTE STATISTICS"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> None, "hr" -> None), noScan = false))
    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c PARTITION(ds, hr) COMPUTE STATISTICS noscan"),
      AnalyzeTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE", allowTempView = false),
        Map("ds" -> None, "hr" -> None), noScan = true))

    val sql1 = "analyze table a.b.c compute statistics xxxx"
    checkError(
      exception = parseException(sql1),
      errorClass = "INVALID_SQL_SYNTAX.ANALYZE_TABLE_UNEXPECTED_NOSCAN",
      parameters = Map("ctx" -> "XXXX"),
      context = ExpectedContext(
        fragment = sql1,
        start = 0,
        stop = 42))

    val sql2 = "analyze table a.b.c partition (a) compute statistics xxxx"
    checkError(
      exception = parseException(sql2),
      errorClass = "INVALID_SQL_SYNTAX.ANALYZE_TABLE_UNEXPECTED_NOSCAN",
      parameters = Map("ctx" -> "XXXX"),
      context = ExpectedContext(
        fragment = sql2,
        start = 0,
        stop = 56))
  }

  test("SPARK-33687: analyze tables statistics") {
    comparePlans(parsePlan("ANALYZE TABLES IN a.b.c COMPUTE STATISTICS"),
      AnalyzeTables(UnresolvedNamespace(Seq("a", "b", "c")), noScan = false))
    comparePlans(parsePlan("ANALYZE TABLES COMPUTE STATISTICS"),
      AnalyzeTables(CurrentNamespace, noScan = false))
    comparePlans(parsePlan("ANALYZE TABLES FROM a COMPUTE STATISTICS NOSCAN"),
      AnalyzeTables(UnresolvedNamespace(Seq("a")), noScan = true))

    val sql = "ANALYZE TABLES IN a.b.c COMPUTE STATISTICS xxxx"
    checkError(
      exception = parseException(sql),
      errorClass = "INVALID_SQL_SYNTAX.ANALYZE_TABLE_UNEXPECTED_NOSCAN",
      parameters = Map("ctx" -> "XXXX"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 46))
  }

  test("analyze table column statistics") {
    val sql1 = "ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR COLUMNS"
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ""))

    comparePlans(
      parsePlan("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR COLUMNS key, value"),
      AnalyzeColumn(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE ... FOR COLUMNS ...", true),
        Option(Seq("key", "value")),
        allColumns = false))

    // Partition specified - should be ignored
    comparePlans(
      parsePlan(
        s"""
           |ANALYZE TABLE a.b.c PARTITION(ds='2017-06-10')
           |COMPUTE STATISTICS FOR COLUMNS key, value
         """.stripMargin),
      AnalyzeColumn(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE ... FOR COLUMNS ...", true),
        Option(Seq("key", "value")),
        allColumns = false))

    // Partition specified should be ignored in case of COMPUTE STATISTICS FOR ALL COLUMNS
    comparePlans(
      parsePlan(
        s"""
           |ANALYZE TABLE a.b.c PARTITION(ds='2017-06-10')
           |COMPUTE STATISTICS FOR ALL COLUMNS
         """.stripMargin),
      AnalyzeColumn(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ANALYZE TABLE ... FOR ALL COLUMNS", true),
        None,
        allColumns = true))

    val sql2 = "ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR ALL COLUMNS key, value"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'key'", "hint" -> "")) // expecting {<EOF>, ';'}

    val sql3 = "ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR ALL"
    checkError(
      exception = parseException(sql3),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ": missing 'COLUMNS'"))
  }

  test("LOAD DATA INTO table") {
    comparePlans(
      parsePlan("LOAD DATA INPATH 'filepath' INTO TABLE a.b.c"),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA"),
        "filepath",
        false,
        false,
        None))

    comparePlans(
      parsePlan("LOAD DATA LOCAL INPATH 'filepath' INTO TABLE a.b.c"),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA"),
        "filepath",
        true,
        false,
        None))

    comparePlans(
      parsePlan("LOAD DATA LOCAL INPATH 'filepath' OVERWRITE INTO TABLE a.b.c"),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA"),
        "filepath",
        true,
        true,
        None))

    comparePlans(
      parsePlan(
        s"""
           |LOAD DATA LOCAL INPATH 'filepath' OVERWRITE INTO TABLE a.b.c
           |PARTITION(ds='2017-06-10')
         """.stripMargin),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA"),
        "filepath",
        true,
        true,
        Some(Map("ds" -> "2017-06-10"))))
  }

  test("CACHE TABLE") {
    comparePlans(
      parsePlan("CACHE TABLE a.b.c"),
      CacheTable(
        UnresolvedRelation(Seq("a", "b", "c")), Seq("a", "b", "c"), false, Map.empty))

    comparePlans(
      parsePlan("CACHE TABLE t AS SELECT * FROM testData"),
      CacheTableAsSelect(
        "t",
        Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("testData"))),
        "SELECT * FROM testData",
        false,
        Map.empty))

    comparePlans(
      parsePlan("CACHE LAZY TABLE a.b.c"),
      CacheTable(
        UnresolvedRelation(Seq("a", "b", "c")), Seq("a", "b", "c"), true, Map.empty))

    comparePlans(
      parsePlan("CACHE LAZY TABLE a.b.c OPTIONS('storageLevel' 'DISK_ONLY')"),
      CacheTable(
        UnresolvedRelation(Seq("a", "b", "c")),
        Seq("a", "b", "c"),
        true,
        Map("storageLevel" -> StorageLevelMapper.DISK_ONLY.name())))

    val sql = "CACHE TABLE a.b.c AS SELECT * FROM testData"
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0037",
      parameters = Map("quoted" -> "a.b"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 42))
  }

  test("SPARK-46610: throw exception when no value for a key in create table options") {
    val createTableSql = "create table test_table using my_data_source options (password)"
    checkError(
      exception = parseException(createTableSql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "A value must be specified for the key: password."),
      context = ExpectedContext(
        fragment = createTableSql,
        start = 0,
        stop = 62))
  }

  test("UNCACHE TABLE") {
    comparePlans(
      parsePlan("UNCACHE TABLE a.b.c"),
      UncacheTable(UnresolvedRelation(Seq("a", "b", "c")), ifExists = false))

    comparePlans(
      parsePlan("UNCACHE TABLE IF EXISTS a.b.c"),
      UncacheTable(UnresolvedRelation(Seq("a", "b", "c")), ifExists = true))
  }

  test("REFRESH TABLE") {
    comparePlans(
      parsePlan("REFRESH TABLE a.b.c"),
      RefreshTable(UnresolvedTableOrView(Seq("a", "b", "c"), "REFRESH TABLE", true)))
  }

  test("show columns") {
    val sql1 = "SHOW COLUMNS FROM t1"
    val sql2 = "SHOW COLUMNS IN db1.t1"
    val sql3 = "SHOW COLUMNS FROM t1 IN db1"
    val sql4 = "SHOW COLUMNS FROM db1.t1 IN db1"

    val parsed1 = parsePlan(sql1)
    val expected1 = ShowColumns(UnresolvedTableOrView(Seq("t1"), "SHOW COLUMNS", true), None)
    val parsed2 = parsePlan(sql2)
    val expected2 = ShowColumns(UnresolvedTableOrView(Seq("db1", "t1"), "SHOW COLUMNS", true), None)
    val parsed3 = parsePlan(sql3)
    val expected3 =
      ShowColumns(UnresolvedTableOrView(Seq("db1", "t1"), "SHOW COLUMNS", true), Some(Seq("db1")))
    val parsed4 = parsePlan(sql4)
    val expected4 =
      ShowColumns(UnresolvedTableOrView(Seq("db1", "t1"), "SHOW COLUMNS", true), Some(Seq("db1")))

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  test("alter view: add partition (not supported)") {
    val sql =
      """ALTER VIEW a.b.c ADD IF NOT EXISTS PARTITION
        |(dt='2008-08-08', country='us') PARTITION
        |(dt='2009-09-09', country='uk')""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER VIEW ... ADD PARTITION"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 117))
  }

  test("alter view: AS Query") {
    val parsed = parsePlan("ALTER VIEW a.b.c AS SELECT 1")
    val expected = AlterViewAs(
      UnresolvedView(Seq("a", "b", "c"), "ALTER VIEW ... AS", true, false),
      "SELECT 1",
      parsePlan("SELECT 1"))
    comparePlans(parsed, expected)
  }

  test("DESCRIBE FUNCTION") {
    def createFuncPlan(name: Seq[String]): UnresolvedFunctionName = {
      UnresolvedFunctionName(name, "DESCRIBE FUNCTION", false, None)
    }
    comparePlans(
      parsePlan("DESC FUNCTION a"),
      DescribeFunction(createFuncPlan(Seq("a")), false))
    comparePlans(
      parsePlan("DESCRIBE FUNCTION a"),
      DescribeFunction(createFuncPlan(Seq("a")), false))
    comparePlans(
      parsePlan("DESCRIBE FUNCTION a.b.c"),
      DescribeFunction(createFuncPlan(Seq("a", "b", "c")), false))
    comparePlans(
      parsePlan("DESCRIBE FUNCTION EXTENDED a.b.c"),
      DescribeFunction(createFuncPlan(Seq("a", "b", "c")), true))
  }

  test("REFRESH FUNCTION") {
    def createFuncPlan(name: Seq[String]): UnresolvedFunctionName = {
      UnresolvedFunctionName(name, "REFRESH FUNCTION", true, None)
    }
    parseCompare("REFRESH FUNCTION c",
      RefreshFunction(createFuncPlan(Seq("c"))))
    parseCompare("REFRESH FUNCTION b.c",
      RefreshFunction(createFuncPlan(Seq("b", "c"))))
    parseCompare("REFRESH FUNCTION a.b.c",
      RefreshFunction(createFuncPlan(Seq("a", "b", "c"))))
  }

  test("CREATE INDEX") {
    parseCompare("CREATE index i1 ON a.b.c USING BTREE (col1)",
      CreateIndex(UnresolvedTable(Seq("a", "b", "c"), "CREATE INDEX"), "i1", "BTREE", false,
        Seq(UnresolvedFieldName(Seq("col1"))).zip(Seq(Map.empty[String, String])), Map.empty))

    parseCompare("CREATE index IF NOT EXISTS i1 ON TABLE a.b.c USING BTREE" +
      " (col1 OPTIONS ('k1'='v1'), col2 OPTIONS ('k2'='v2')) ",
      CreateIndex(UnresolvedTable(Seq("a", "b", "c"), "CREATE INDEX"), "i1", "BTREE", true,
        Seq(UnresolvedFieldName(Seq("col1")), UnresolvedFieldName(Seq("col2")))
          .zip(Seq(Map("k1" -> "v1"), Map("k2" -> "v2"))), Map.empty))

    parseCompare("CREATE index i1 ON a.b.c" +
      " (col1 OPTIONS ('k1'='v1'), col2 OPTIONS ('k2'='v2')) OPTIONS ('k3'='v3', 'k4'='v4')",
      CreateIndex(UnresolvedTable(Seq("a", "b", "c"), "CREATE INDEX"), "i1", "", false,
        Seq(UnresolvedFieldName(Seq("col1")), UnresolvedFieldName(Seq("col2")))
          .zip(Seq(Map("k1" -> "v1"), Map("k2" -> "v2"))), Map("k3" -> "v3", "k4" -> "v4")))
  }

  test("DROP INDEX") {
    parseCompare("DROP index i1 ON a.b.c",
      DropIndex(UnresolvedTable(Seq("a", "b", "c"), "DROP INDEX"), "i1", false))

    parseCompare("DROP index IF EXISTS i1 ON a.b.c",
      DropIndex(UnresolvedTable(Seq("a", "b", "c"), "DROP INDEX"), "i1", true))
  }

  private case class TableSpec(
      name: Seq[String],
      columns: Option[Seq[ColumnDefinition]],
      partitioning: Seq[Transform],
      properties: Map[String, String],
      provider: Option[String],
      options: OptionList,
      location: Option[String],
      comment: Option[String],
      serdeInfo: Option[SerdeInfo],
      external: Boolean = false)

  private object TableSpec {
    def apply(plan: LogicalPlan): TableSpec = {
      plan match {
        case create: CreateTable =>
          val tableSpec = create.tableSpec.asInstanceOf[UnresolvedTableSpec]
          TableSpec(
            create.name.asInstanceOf[UnresolvedIdentifier].nameParts,
            Some(create.columns),
            create.partitioning,
            tableSpec.properties,
            tableSpec.provider,
            tableSpec.optionExpression,
            tableSpec.location,
            tableSpec.comment,
            tableSpec.serde,
            tableSpec.external)
        case replace: ReplaceTable =>
          val tableSpec = replace.tableSpec.asInstanceOf[UnresolvedTableSpec]
          TableSpec(
            replace.name.asInstanceOf[UnresolvedIdentifier].nameParts,
            Some(replace.columns),
            replace.partitioning,
            tableSpec.properties,
            tableSpec.provider,
            tableSpec.optionExpression,
            tableSpec.location,
            tableSpec.comment,
            tableSpec.serde)
        case ctas: CreateTableAsSelect =>
          val tableSpec = ctas.tableSpec.asInstanceOf[UnresolvedTableSpec]
          TableSpec(
            ctas.name.asInstanceOf[UnresolvedIdentifier].nameParts,
            if (ctas.query.resolved) Some(ctas.columns) else None,
            ctas.partitioning,
            tableSpec.properties,
            tableSpec.provider,
            tableSpec.optionExpression,
            tableSpec.location,
            tableSpec.comment,
            tableSpec.serde,
            tableSpec.external)
        case rtas: ReplaceTableAsSelect =>
          val tableSpec = rtas.tableSpec.asInstanceOf[UnresolvedTableSpec]
          TableSpec(
            rtas.name.asInstanceOf[UnresolvedIdentifier].nameParts,
            if (rtas.query.resolved) Some(rtas.columns) else None,
            rtas.partitioning,
            tableSpec.properties,
            tableSpec.provider,
            tableSpec.optionExpression,
            tableSpec.location,
            tableSpec.comment,
            tableSpec.serde)
        case other =>
          fail(s"Expected to parse Create, CTAS, Replace, or RTAS plan" +
            s" from query, got ${other.getClass.getName}.")
      }
    }
  }

  test("comment on") {
    comparePlans(
      parsePlan("COMMENT ON DATABASE a.b.c IS NULL"),
      CommentOnNamespace(UnresolvedNamespace(Seq("a", "b", "c")), ""))

    comparePlans(
      parsePlan("COMMENT ON DATABASE a.b.c IS 'NULL'"),
      CommentOnNamespace(UnresolvedNamespace(Seq("a", "b", "c")), "NULL"))

    comparePlans(
      parsePlan("COMMENT ON NAMESPACE a.b.c IS ''"),
      CommentOnNamespace(UnresolvedNamespace(Seq("a", "b", "c")), ""))

    comparePlans(
      parsePlan("COMMENT ON TABLE a.b.c IS 'xYz'"),
      CommentOnTable(UnresolvedTable(Seq("a", "b", "c"), "COMMENT ON TABLE"), "xYz"))
  }

  test("create table - without using") {
    val sql = "CREATE TABLE 1m.2g(a INT)"
    val expectedTableSpec = TableSpec(
      Seq("1m", "2g"),
      Some(Seq(ColumnDefinition("a", IntegerType))),
      Seq.empty[Transform],
      Map.empty[String, String],
      None,
      OptionList(Seq.empty),
      None,
      None,
      None)

    testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    def insertPartitionPlan(part: String, optimizeInsertIntoCmds: Boolean): InsertIntoStatement = {
      InsertIntoStatement(
        UnresolvedRelation(Seq("t")),
        Map("part" -> Some(part)),
        Seq.empty[String],
        if (optimizeInsertIntoCmds) {
          ResolveInlineTables(UnresolvedInlineTable(Seq("col1"), Seq(Seq(Literal("a")))))
        } else {
          UnresolvedInlineTable(Seq("col1"), Seq(Seq(Literal("a"))))
        },
        overwrite = false, ifPartitionNotExists = false)
    }
    val binaryStr = "Spark SQL"
    val binaryHexStr = Hex.hex(UTF8String.fromString(binaryStr).getBytes).toString
    val dateTypeSql = "INSERT INTO t PARTITION(part = date'2019-01-02') VALUES('a')"
    val interval = new CalendarInterval(7, 1, 1000).toString
    val intervalTypeSql = s"INSERT INTO t PARTITION(part = interval'$interval') VALUES('a')"
    val ymIntervalTypeSql = "INSERT INTO t PARTITION(part = interval'1 year 2 month') VALUES('a')"
    val dtIntervalTypeSql = "INSERT INTO t PARTITION(part = interval'1 day 2 hour " +
      "3 minute 4.123456 second 5 millisecond 6 microsecond') VALUES('a')"
    val timestamp = "2019-01-02 11:11:11"
    val timestampTypeSql = s"INSERT INTO t PARTITION(part = timestamp'$timestamp') VALUES('a')"
    val binaryTypeSql = s"INSERT INTO t PARTITION(part = X'$binaryHexStr') VALUES('a')"

    for (optimizeInsertIntoValues <- Seq(true, false)) {
      withSQLConf(
        SQLConf.OPTIMIZE_INSERT_INTO_VALUES_PARSER.key ->
          optimizeInsertIntoValues.toString) {
        comparePlans(parsePlan(dateTypeSql), insertPartitionPlan(
          "2019-01-02", optimizeInsertIntoValues))
        withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
          comparePlans(parsePlan(intervalTypeSql), insertPartitionPlan(
            interval, optimizeInsertIntoValues))
        }
        comparePlans(parsePlan(ymIntervalTypeSql), insertPartitionPlan(
          "INTERVAL '1-2' YEAR TO MONTH", optimizeInsertIntoValues))
        comparePlans(parsePlan(dtIntervalTypeSql),
          insertPartitionPlan(
            "INTERVAL '1 02:03:04.128462' DAY TO SECOND", optimizeInsertIntoValues))
        comparePlans(parsePlan(timestampTypeSql), insertPartitionPlan(
          timestamp, optimizeInsertIntoValues))
        comparePlans(parsePlan(binaryTypeSql), insertPartitionPlan(
          binaryStr, optimizeInsertIntoValues))
      }
    }
  }

  test("SPARK-38335: Implement parser support for DEFAULT values for columns in tables") {
    // These CREATE/REPLACE TABLE statements should parse successfully.
    val columnsWithDefaultValue = Seq(
      ColumnDefinition("a", IntegerType),
      ColumnDefinition(
        "b",
        StringType,
        nullable = false,
        defaultValue = Some(DefaultValueExpression(Literal("abc"), "'abc'")))
    )
    val createTableResult =
      CreateTable(UnresolvedIdentifier(Seq("my_tab")), columnsWithDefaultValue,
        Seq.empty[Transform], UnresolvedTableSpec(Map.empty[String, String], Some("parquet"),
         OptionList(Seq.empty), None, None, None, false), false)
    // Parse the CREATE TABLE statement twice, swapping the order of the NOT NULL and DEFAULT
    // options, to make sure that the parser accepts any ordering of these options.
    comparePlans(parsePlan(
      "CREATE TABLE my_tab(a INT, b STRING NOT NULL DEFAULT 'abc') USING parquet"),
      createTableResult)
    comparePlans(parsePlan(
      "CREATE TABLE my_tab(a INT, b STRING DEFAULT 'abc' NOT NULL) USING parquet"),
      createTableResult)
    comparePlans(parsePlan("REPLACE TABLE my_tab(a INT, " +
      "b STRING NOT NULL DEFAULT 'abc') USING parquet"),
      ReplaceTable(UnresolvedIdentifier(Seq("my_tab")), columnsWithDefaultValue,
        Seq.empty[Transform], UnresolvedTableSpec(Map.empty[String, String], Some("parquet"),
          OptionList(Seq.empty), None, None, None, false), false))
    // These ALTER TABLE statements should parse successfully.
    comparePlans(
      parsePlan("ALTER TABLE t1 ADD COLUMN x int NOT NULL DEFAULT 42"),
      AddColumns(UnresolvedTable(Seq("t1"), "ALTER TABLE ... ADD COLUMN"),
        Seq(QualifiedColType(None, "x", IntegerType, false, None, None, Some("42")))))
    comparePlans(
      parsePlan("ALTER TABLE t1 ALTER COLUMN a.b.c SET DEFAULT 42"),
      AlterColumn(
        UnresolvedTable(Seq("t1"), "ALTER TABLE ... ALTER COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        None,
        None,
        Some("42")))
    // It is possible to pass an empty string default value using quotes.
    comparePlans(
      parsePlan("ALTER TABLE t1 ALTER COLUMN a.b.c SET DEFAULT ''"),
      AlterColumn(
        UnresolvedTable(Seq("t1"), "ALTER TABLE ... ALTER COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        None,
        None,
        Some("''")))
    // It is not possible to pass an empty string default value without using quotes.
    // This results in a parsing error.
    val sql1 = "ALTER TABLE t1 ALTER COLUMN a.b.c SET DEFAULT "
    checkError(
      exception = parseException(sql1),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ""))
    // It is not possible to both SET DEFAULT and DROP DEFAULT at the same time.
    // This results in a parsing error.
    val sql2 = "ALTER TABLE t1 ALTER COLUMN a.b.c DROP DEFAULT SET DEFAULT 42"
    checkError(
      exception = parseException(sql2),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'SET'", "hint" -> ""))

    comparePlans(
      parsePlan("ALTER TABLE t1 ALTER COLUMN a.b.c DROP DEFAULT"),
      AlterColumn(
        UnresolvedTable(Seq("t1"), "ALTER TABLE ... ALTER COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        None,
        None,
        Some("")))
    // Make sure that the parser returns an exception when the feature is disabled.
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
      val sql = "CREATE TABLE my_tab(a INT, b STRING NOT NULL DEFAULT \"abc\") USING parquet"
      val fragment = "b STRING NOT NULL DEFAULT \"abc\""
      checkError(
        exception = parseException(sql),
        errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITH_SUGGESTION",
        parameters = Map.empty,
        context = ExpectedContext(
          fragment = fragment,
          start = 27,
          stop = 57))
    }

    // In each of the following cases, the DEFAULT reference parses as an unresolved attribute
    // reference. We can handle these cases after the parsing stage, at later phases of analysis.
    comparePlans(parsePlan("VALUES (1, 2, DEFAULT) AS val"),
      SubqueryAlias("val",
        UnresolvedInlineTable(Seq("col1", "col2", "col3"), Seq(Seq(Literal(1), Literal(2),
          UnresolvedAttribute("DEFAULT"))))))
    comparePlans(parsePlan(
      "INSERT INTO t PARTITION(part = date'2019-01-02') VALUES ('a', DEFAULT)"),
      InsertIntoStatement(
        UnresolvedRelation(Seq("t")),
        Map("part" -> Some("2019-01-02")),
        userSpecifiedCols = Seq.empty[String],
        query = UnresolvedInlineTable(Seq("col1", "col2"), Seq(Seq(Literal("a"),
          UnresolvedAttribute("DEFAULT")))),
        overwrite = false, ifPartitionNotExists = false))
    parseCompare(
      """
        |MERGE INTO testcat1.ns1.ns2.tbl AS target
        |USING testcat2.ns1.ns2.tbl AS source
        |ON target.col1 = source.col1
        |WHEN MATCHED AND (target.col2='delete') THEN DELETE
        |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = DEFAULT
        |WHEN NOT MATCHED AND (target.col2='insert')
        |THEN INSERT (target.col1, target.col2) VALUES (source.col1, DEFAULT)
        |WHEN NOT MATCHED BY SOURCE AND (target.col2='delete') THEN DELETE
        |WHEN NOT MATCHED BY SOURCE AND (target.col2='update') THEN UPDATE SET target.col2 = DEFAULT
      """.stripMargin,
      MergeIntoTable(
        SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
        SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
        EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("DEFAULT"))))),
        Seq(InsertAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert"))),
          Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("DEFAULT"))))),
        Seq(DeleteAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("delete")))),
          UpdateAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("update"))),
            Seq(Assignment(UnresolvedAttribute("target.col2"),
              UnresolvedAttribute("DEFAULT"))))),
        withSchemaEvolution = false))
  }

  test("SPARK-40944: Relax ordering constraint for CREATE TABLE column options") {
    // These are negative test cases exercising error cases. Note that positive test cases
    // exercising flexible column option ordering exist elsewhere in this suite.
    checkError(
      exception = intercept[ParseException](
        parsePlan(
          "CREATE TABLE my_tab(a INT, b STRING NOT NULL DEFAULT \"abc\" NOT NULL)")),
      errorClass = "CREATE_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map(
        "columnName" -> "b",
        "optionName" -> "NOT NULL"),
      context = ExpectedContext(
        fragment = "b STRING NOT NULL DEFAULT \"abc\" NOT NULL", start = 27, stop = 66))
    checkError(
      exception = intercept[ParseException](
        parsePlan(
          "CREATE TABLE my_tab(a INT, b STRING DEFAULT \"123\" NOT NULL DEFAULT \"abc\")")),
      errorClass = "CREATE_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map(
        "columnName" -> "b",
        "optionName" -> "DEFAULT"),
      context = ExpectedContext(
        fragment = "b STRING DEFAULT \"123\" NOT NULL DEFAULT \"abc\"", start = 27, stop = 71))
    checkError(
      exception = intercept[ParseException](
        parsePlan(
          "CREATE TABLE my_tab(a INT, b STRING COMMENT \"abc\" NOT NULL COMMENT \"abc\")")),
      errorClass = "CREATE_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map(
        "columnName" -> "b",
        "optionName" -> "COMMENT"),
      context = ExpectedContext(
        fragment = "b STRING COMMENT \"abc\" NOT NULL COMMENT \"abc\"", start = 27, stop = 71))
  }

  test("SPARK-41290: implement parser support for GENERATED ALWAYS AS columns in tables") {
    val columnsWithGenerationExpr = Seq(
      ColumnDefinition("a", IntegerType),
      ColumnDefinition(
        "b",
        IntegerType,
        nullable = false,
        generationExpression = Some("a+1")
      )
    )
    comparePlans(parsePlan(
      "CREATE TABLE my_tab(a INT, b INT NOT NULL GENERATED ALWAYS AS (a+1)) USING parquet"),
      CreateTable(UnresolvedIdentifier(Seq("my_tab")), columnsWithGenerationExpr,
        Seq.empty[Transform], UnresolvedTableSpec(Map.empty[String, String], Some("parquet"),
          OptionList(Seq.empty), None, None, None, false), false))
    comparePlans(parsePlan(
      "REPLACE TABLE my_tab(a INT, b INT NOT NULL GENERATED ALWAYS AS (a+1)) USING parquet"),
      ReplaceTable(UnresolvedIdentifier(Seq("my_tab")), columnsWithGenerationExpr,
        Seq.empty[Transform], UnresolvedTableSpec(Map.empty[String, String], Some("parquet"),
          OptionList(Seq.empty), None, None, None, false), false))
    // Two generation expressions
    checkError(
      exception = parseException("CREATE TABLE my_tab(a INT, " +
          "b INT GENERATED ALWAYS AS (a + 1) GENERATED ALWAYS AS (a + 2)) USING PARQUET"),
      errorClass = "CREATE_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map("columnName" -> "b", "optionName" -> "GENERATED ALWAYS AS"),
      context = ExpectedContext(
        fragment = "b INT GENERATED ALWAYS AS (a + 1) GENERATED ALWAYS AS (a + 2)",
        start = 27,
        stop = 87
      )
    )
    // Empty expression
    checkError(
      exception = parseException(
        "CREATE TABLE my_tab(a INT, b INT GENERATED ALWAYS AS ()) USING PARQUET"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "')'", "hint" -> "")
    )
    // No parenthesis
    checkError(
      exception = parseException(
        "CREATE TABLE my_tab(a INT, b INT GENERATED ALWAYS AS a + 1) USING PARQUET"),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'a'", "hint" -> ": missing '('")
    )
  }

  test("SPARK-42681: Relax ordering constraint for ALTER TABLE ADD COLUMN options") {
    // Positive test cases to verify that column definition options could be applied in any order.
    val expectedPlan = AddColumns(
      UnresolvedTable(Seq("my_tab"), "ALTER TABLE ... ADD COLUMN"),
      Seq(
        QualifiedColType(
          path = None,
          colName = "x",
          dataType = StringType,
          nullable = false,
          comment = Some("a"),
          position = Some(UnresolvedFieldPosition(first())),
          default = Some("'abc'")
        )
      )
    )
    Seq("NOT NULL", "COMMENT \"a\"", "FIRST", "DEFAULT 'abc'").permutations
      .map(_.mkString(" "))
      .foreach { element =>
        parseCompare(s"ALTER TABLE my_tab ADD COLUMN x STRING $element", expectedPlan)
      }

    // These are negative test cases exercising error cases.
    checkError(
      exception = intercept[ParseException](
        parsePlan("ALTER TABLE my_tab ADD COLUMN b STRING NOT NULL DEFAULT \"abc\" NOT NULL")
      ),
      errorClass = "ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map("type" -> "ADD", "columnName" -> "b", "optionName" -> "NOT NULL"),
      context = ExpectedContext(
        fragment = "b STRING NOT NULL DEFAULT \"abc\" NOT NULL",
        start = 30,
        stop = 69
      )
    )
    checkError(
      exception = intercept[ParseException](
        parsePlan("ALTER TABLE my_tab ADD COLUMN b STRING DEFAULT \"123\" NOT NULL DEFAULT \"abc\"")
      ),
      errorClass = "ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map("type" -> "ADD", "columnName" -> "b", "optionName" -> "DEFAULT"),
      context = ExpectedContext(
        fragment = "b STRING DEFAULT \"123\" NOT NULL DEFAULT \"abc\"",
        start = 30,
        stop = 74
      )
    )
    checkError(
      exception = intercept[ParseException](
        parsePlan("ALTER TABLE my_tab ADD COLUMN b STRING COMMENT \"abc\" NOT NULL COMMENT \"abc\"")
      ),
      errorClass = "ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map("type" -> "ADD", "columnName" -> "b", "optionName" -> "COMMENT"),
      context = ExpectedContext(
        fragment = "b STRING COMMENT \"abc\" NOT NULL COMMENT \"abc\"",
        start = 30,
        stop = 74
      )
    )
    checkError(
      exception = intercept[ParseException](
        parsePlan("ALTER TABLE my_tab ADD COLUMN b STRING FIRST COMMENT \"abc\" AFTER y")
      ),
      errorClass = "ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE",
      parameters = Map("type" -> "ADD", "columnName" -> "b", "optionName" -> "FIRST|AFTER"),
      context =
        ExpectedContext(fragment = "b STRING FIRST COMMENT \"abc\" AFTER y", start = 30, stop = 65)
    )
  }

  test("create table cluster by with bucket") {
    val sql1 = "CREATE TABLE my_tab(a INT, b STRING) " +
      "USING parquet CLUSTERED BY (a) INTO 2 BUCKETS CLUSTER BY (a)"
    checkError(
      exception = parseException(sql1),
      errorClass = "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED",
      parameters = Map.empty,
      context = ExpectedContext(fragment = sql1, start = 0, stop = 96)
    )
  }

  test("replace table cluster by with bucket") {
    val sql1 = "REPLACE TABLE my_tab(a INT, b STRING) " +
      "USING parquet CLUSTERED BY (a) INTO 2 BUCKETS CLUSTER BY (a)"
    checkError(
      exception = parseException(sql1),
      errorClass = "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED",
      parameters = Map.empty,
      context = ExpectedContext(fragment = sql1, start = 0, stop = 97)
    )
  }

  test("create table cluster by with partitioned by") {
    val sql1 = "CREATE TABLE my_tab(a INT, b STRING) " +
      "USING parquet CLUSTER BY (a) PARTITIONED BY (a)"
    checkError(
      exception = parseException(sql1),
      errorClass = "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
      parameters = Map.empty,
      context = ExpectedContext(fragment = sql1, start = 0, stop = 83)
    )
  }

  test("replace table cluster by with partitioned by") {
    val sql1 = "REPLACE TABLE my_tab(a INT, b STRING) " +
      "USING parquet CLUSTER BY (a) PARTITIONED BY (a)"
    checkError(
      exception = parseException(sql1),
      errorClass = "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
      parameters = Map.empty,
      context = ExpectedContext(fragment = sql1, start = 0, stop = 84)
    )
  }

  test("AstBuilder don't support `INSERT OVERWRITE DIRECTORY`") {
    val insertDirSql =
      s"""
         | INSERT OVERWRITE LOCAL DIRECTORY
         | USING parquet
         | OPTIONS (
         |  path 'xxx'
         | )
         | SELECT i from t1""".stripMargin

    checkError(
      exception = internalException(insertDirSql),
      errorClass = "INTERNAL_ERROR",
      parameters = Map("message" -> "INSERT OVERWRITE DIRECTORY is not supported."))
  }
}
