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

import java.time.DateTimeException
import java.util
import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Hex, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition.{after, first}
import org.apache.spark.sql.connector.expressions.{ApplyTransform, BucketTransform, DaysTransform, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, TimeTravelSpec, Transform, YearsTransform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class DDLParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)

  private def parseCompare(sql: String, expected: LogicalPlan): Unit = {
    comparePlans(parsePlan(sql), expected, checkAnalysis = false)
  }

  test("create/replace table using - schema") {
    val createSql = "CREATE TABLE my_tab(a INT COMMENT 'test', b STRING NOT NULL) USING parquet"
    val replaceSql = "REPLACE TABLE my_tab(a INT COMMENT 'test', b STRING NOT NULL) USING parquet"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType()
        .add("a", IntegerType, nullable = true, "test")
        .add("b", StringType, nullable = false)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
      None,
      None,
      None)

    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }

    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING) USING parquet",
      "extraneous input ':'")
  }

  test("create/replace table - with IF NOT EXISTS") {
    val sql = "CREATE TABLE IF NOT EXISTS my_tab(a INT, b STRING) USING parquet"
    testCreateOrReplaceDdl(
      sql,
      TableSpec(
        Seq("my_tab"),
        Some(new StructType().add("a", IntegerType).add("b", StringType)),
        Seq.empty[Transform],
        None,
        Map.empty[String, String],
        Some("parquet"),
        Map.empty[String, String],
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
      Some(new StructType()
        .add("a", IntegerType, nullable = true, "test")
        .add("b", StringType)),
      Seq(IdentityTransform(FieldReference("a"))),
      None,
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
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
      Some(new StructType()
        .add("a", IntegerType)
        .add("b", StringType)
        .add("ts", TimestampType)),
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
      None,
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
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
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      Seq.empty[Transform],
      Some(BucketSpec(5, Seq("a"), Seq("b"))),
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - with comment") {
    val createSql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"
    val replaceSql = "REPLACE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"
    val expectedTableSpec = TableSpec(
      Seq("my_tab"),
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
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
      Some(new StructType().add("a", IntegerType).add("b", StringType)),
      Seq.empty[Transform],
      None,
      Map("test" -> "test"),
      Some("parquet"),
      Map.empty[String, String],
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
        Some(new StructType().add("a", IntegerType).add("b", StringType)),
        Seq.empty[Transform],
        None,
        Map.empty[String, String],
        Some("parquet"),
        Map.empty[String, String],
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
      Some(new StructType().add("a", IntegerType)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
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
      Some(new StructType().add("id", LongType).add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
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
      Some(new StructType().add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
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
      Some(new StructType().add("id", LongType).add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      Some("parquet"),
      Map.empty[String, String],
      None,
      None,
      None)
    Seq(createSql, replaceSql).foreach { sql =>
      testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
    }
  }

  test("create/replace table - mixed partition references and column definitions") {
    val createSql = "CREATE TABLE my_tab (id bigint, p1 string) PARTITIONED BY (p1, p2 string)"
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    Seq(createSql, replaceSql).foreach { sql =>
      assertUnsupported(sql, Seq(
        "PARTITION BY: Cannot mix partition expressions and partition columns",
        "Expressions: p1",
        "Columns: p2 string"))
    }

    val createSqlWithExpr =
      "CREATE TABLE my_tab (id bigint, p1 string) PARTITIONED BY (p2 string, truncate(p1, 16))"
    val replaceSqlWithExpr = createSqlWithExpr.replaceFirst("CREATE", "REPLACE")
    Seq(createSqlWithExpr, replaceSqlWithExpr).foreach { sql =>
      assertUnsupported(sql, Seq(
        "PARTITION BY: Cannot mix partition expressions and partition columns",
        "Expressions: truncate(p1, 16)",
        "Columns: p2 string"))
    }
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
      Some(new StructType().add("id", LongType).add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
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
        Some(new StructType().add("id", LongType).add("part", StringType)),
        Seq(IdentityTransform(FieldReference("part"))),
        None,
        Map.empty[String, String],
        None,
        Map.empty[String, String],
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
         |WITH SERDEPROPERTIES ('prop'='value')
         """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    Seq(createSql, replaceSql).foreach { sql =>
      assertUnsupported(sql, Seq("ROW FORMAT SERDE is incompatible with format 'otherFormat'"))
    }
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
      Some(new StructType().add("id", LongType).add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
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
         |FIELDS TERMINATED BY ','
         """.stripMargin
    val replaceFailSql = createFailSql.replaceFirst("CREATE", "REPLACE")
    Seq(createFailSql, replaceFailSql).foreach { sql =>
      assertUnsupported(sql, Seq(
        "ROW FORMAT DELIMITED is only compatible with 'textfile', not 'otherFormat'"))
    }
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
      Some(new StructType().add("id", LongType).add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
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
      Some(new StructType().add("id", LongType).add("part", StringType)),
      Seq(IdentityTransform(FieldReference("part"))),
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
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
        |STORED AS parquet
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    Seq(createSql, replaceSql).foreach { sql =>
      assertUnsupported(sql, Seq("CREATE TABLE ... USING ... STORED AS"))
    }
  }

  test("create/replace table - using with row format serde") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, part string)
        |USING parquet
        |ROW FORMAT SERDE 'customSerde'
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    Seq(createSql, replaceSql).foreach { sql =>
      assertUnsupported(sql, Seq("CREATE TABLE ... USING ... ROW FORMAT SERDE"))
    }
  }

  test("create/replace table - using with row format delimited") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, part string)
        |USING parquet
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    Seq(createSql, replaceSql).foreach { sql =>
      assertUnsupported(sql, Seq("CREATE TABLE ... USING ... ROW FORMAT DELIMITED"))
    }
  }

  test("create/replace table - stored by") {
    val createSql =
      """CREATE TABLE my_tab (id bigint, p1 string)
        |STORED BY 'handler'
        """.stripMargin
    val replaceSql = createSql.replaceFirst("CREATE", "REPLACE")
    Seq(createSql, replaceSql).foreach { sql =>
      assertUnsupported(sql, Seq("stored by"))
    }
  }

  test("Unsupported skew clause - create/replace table") {
    intercept("CREATE TABLE my_tab (id bigint) SKEWED BY (id) ON (1,2,3)",
      "CREATE TABLE ... SKEWED BY")
    intercept("REPLACE TABLE my_tab (id bigint) SKEWED BY (id) ON (1,2,3)",
      "CREATE TABLE ... SKEWED BY")
  }

  test("Duplicate clauses - create/replace table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) $duplicateClause $duplicateClause"
    }

    def replaceTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) $duplicateClause $duplicateClause"
    }

    intercept(createTableHeader("TBLPROPERTIES('test' = 'test2')"),
      "Found duplicate clauses: TBLPROPERTIES")
    intercept(createTableHeader("LOCATION '/tmp/file'"),
      "Found duplicate clauses: LOCATION")
    intercept(createTableHeader("COMMENT 'a table'"),
      "Found duplicate clauses: COMMENT")
    intercept(createTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS"),
      "Found duplicate clauses: CLUSTERED BY")
    intercept(createTableHeader("PARTITIONED BY (b)"),
      "Found duplicate clauses: PARTITIONED BY")
    intercept(createTableHeader("PARTITIONED BY (c int)"),
      "Found duplicate clauses: PARTITIONED BY")
    intercept(createTableHeader("STORED AS parquet"),
      "Found duplicate clauses: STORED AS")
    intercept(createTableHeader("STORED AS INPUTFORMAT 'in' OUTPUTFORMAT 'out'"),
      "Found duplicate clauses: STORED AS")
    intercept(createTableHeader("ROW FORMAT SERDE 'serde'"),
      "Found duplicate clauses: ROW FORMAT")

    intercept(replaceTableHeader("TBLPROPERTIES('test' = 'test2')"),
      "Found duplicate clauses: TBLPROPERTIES")
    intercept(replaceTableHeader("LOCATION '/tmp/file'"),
      "Found duplicate clauses: LOCATION")
    intercept(replaceTableHeader("COMMENT 'a table'"),
      "Found duplicate clauses: COMMENT")
    intercept(replaceTableHeader("CLUSTERED BY(b) INTO 256 BUCKETS"),
      "Found duplicate clauses: CLUSTERED BY")
    intercept(replaceTableHeader("PARTITIONED BY (b)"),
      "Found duplicate clauses: PARTITIONED BY")
    intercept(replaceTableHeader("PARTITIONED BY (c int)"),
      "Found duplicate clauses: PARTITIONED BY")
    intercept(replaceTableHeader("STORED AS parquet"),
      "Found duplicate clauses: STORED AS")
    intercept(replaceTableHeader("STORED AS INPUTFORMAT 'in' OUTPUTFORMAT 'out'"),
      "Found duplicate clauses: STORED AS")
    intercept(replaceTableHeader("ROW FORMAT SERDE 'serde'"),
      "Found duplicate clauses: ROW FORMAT")
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
          Some(new StructType),
          Seq.empty[Transform],
          Option.empty[BucketSpec],
          Map.empty[String, String],
          Some("json"),
          Map("a" -> "1", "b" -> "0.1", "c" -> "true"),
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
        None,
        Map("p1" -> "v1", "p2" -> "v2"),
        Some("parquet"),
        Map.empty[String, String],
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
      DropView(UnresolvedView(Seq("testcat", "db", "view"), cmd, true, hint), ifExists = false))
    parseCompare(s"DROP VIEW db.view",
      DropView(UnresolvedView(Seq("db", "view"), cmd, true, hint), ifExists = false))
    parseCompare(s"DROP VIEW IF EXISTS db.view",
      DropView(UnresolvedView(Seq("db", "view"), cmd, true, hint), ifExists = true))
    parseCompare(s"DROP VIEW view",
      DropView(UnresolvedView(Seq("view"), cmd, true, hint), ifExists = false))
    parseCompare(s"DROP VIEW IF EXISTS view",
      DropView(UnresolvedView(Seq("view"), cmd, true, hint), ifExists = true))
  }

  private def testCreateOrReplaceDdl(
      sqlStatement: String,
      tableSpec: TableSpec,
      expectedIfNotExists: Boolean): Unit = {
    val parsedPlan = parsePlan(sqlStatement)
    val newTableToken = sqlStatement.split(" ")(0).trim.toUpperCase(Locale.ROOT)
    parsedPlan match {
      case create: CreateTableStatement if newTableToken == "CREATE" =>
        assert(create.ifNotExists == expectedIfNotExists)
      case ctas: CreateTableAsSelectStatement if newTableToken == "CREATE" =>
        assert(ctas.ifNotExists == expectedIfNotExists)
      case replace: ReplaceTableStatement if newTableToken == "REPLACE" =>
      case replace: ReplaceTableAsSelectStatement if newTableToken == "REPLACE" =>
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
    val hint = Some("Please use ALTER TABLE instead.")

    comparePlans(parsePlan(sql1_view),
      SetViewProperties(
        UnresolvedView(Seq("table_name"), "ALTER VIEW ... SET TBLPROPERTIES", false, hint),
        Map("test" -> "test", "comment" -> "new_comment")))
    comparePlans(parsePlan(sql2_view),
      UnsetViewProperties(
        UnresolvedView(Seq("table_name"), "ALTER VIEW ... UNSET TBLPROPERTIES", false, hint),
        Seq("comment", "test"),
        ifExists = false))
    comparePlans(parsePlan(sql3_view),
      UnsetViewProperties(
        UnresolvedView(Seq("table_name"), "ALTER VIEW ... UNSET TBLPROPERTIES", false, hint),
        Seq("comment", "test"),
        ifExists = true))
  }

  // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER TABLE table_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter table: alter table properties") {
    val sql1_table = "ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
        "'comment' = 'new_comment')"
    val sql2_table = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_table = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"
    val hint = Some("Please use ALTER VIEW instead.")

    comparePlans(
      parsePlan(sql1_table),
      SetTableProperties(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET TBLPROPERTIES", hint),
        Map("test" -> "test", "comment" -> "new_comment")))
    comparePlans(
      parsePlan(sql2_table),
      UnsetTableProperties(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... UNSET TBLPROPERTIES", hint),
        Seq("comment", "test"),
        ifExists = false))
    comparePlans(
      parsePlan(sql3_table),
      UnsetTableProperties(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... UNSET TBLPROPERTIES", hint),
        Seq("comment", "test"),
        ifExists = true))
  }

  test("alter table: add column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None)
      )))
  }

  test("alter table: add multiple columns") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS x int, y string"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS", None),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None),
          QualifiedColType(None, "y", StringType, true, None, None)
      )))
  }

  test("alter table: add column with COLUMNS") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS x int"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS", None),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None)
      )))
  }

  test("alter table: add column with COLUMNS (...)") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS (x int)"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS", None),
        Seq(QualifiedColType(None, "x", IntegerType, true, None, None)
      )))
  }

  test("alter table: add column with COLUMNS (...) and COMMENT") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMNS (x int COMMENT 'doc')"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMNS", None),
        Seq(QualifiedColType(None, "x", IntegerType, true, Some("doc"), None)
      )))
  }

  test("alter table: add non-nullable column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int NOT NULL"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(QualifiedColType(None, "x", IntegerType, false, None, None)
      )))
  }

  test("alter table: add column with COMMENT") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int COMMENT 'doc'"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(QualifiedColType(None, "x", IntegerType, true, Some("doc"), None)
      )))
  }

  test("alter table: add column with position") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int FIRST"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(QualifiedColType(
          None,
          "x",
          IntegerType,
          true,
          None,
          Some(UnresolvedFieldPosition(first())))
      )))

    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x int AFTER y"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(QualifiedColType(
          None,
          "x",
          IntegerType,
          true,
          None,
          Some(UnresolvedFieldPosition(after("y"))))
      )))
  }

  test("alter table: add column with nested column name") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x.y.z int COMMENT 'doc'"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(QualifiedColType(
          Some(UnresolvedFieldName(Seq("x", "y"))), "z", IntegerType, true, Some("doc"), None)
      )))
  }

  test("alter table: add multiple columns with nested column name") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ADD COLUMN x.y.z int COMMENT 'doc', a.b string FIRST"),
      AddColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ADD COLUMN", None),
        Seq(
          QualifiedColType(
            Some(UnresolvedFieldName(Seq("x", "y"))),
            "z",
            IntegerType,
            true,
            Some("doc"),
            None),
          QualifiedColType(
            Some(UnresolvedFieldName(Seq("a"))),
            "b",
            StringType,
            true,
            None,
            Some(UnresolvedFieldPosition(first())))
      )))
  }

  test("alter table: set location") {
    val hint = Some("Please use ALTER VIEW instead.")
    comparePlans(
      parsePlan("ALTER TABLE a.b.c SET LOCATION 'new location'"),
      SetTableLocation(
        UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET LOCATION ...", hint),
        None,
        "new location"))

    comparePlans(
      parsePlan("ALTER TABLE a.b.c PARTITION(ds='2017-06-10') SET LOCATION 'new location'"),
      SetTableLocation(
        UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET LOCATION ...", hint),
        Some(Map("ds" -> "2017-06-10")),
        "new location"))
  }

  test("alter table: rename column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name RENAME COLUMN a.b.c TO d"),
      RenameColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... RENAME COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        "d"))
  }

  test("alter table: update column type using ALTER") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bigint"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ALTER COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(LongType),
        None,
        None,
        None))
  }

  test("alter table: update column type invalid type") {
    val msg = intercept[ParseException] {
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c TYPE bad_type")
    }.getMessage
    assert(msg.contains("DataType bad_type is not supported"))
  }

  test("alter table: update column type") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c TYPE bigint"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(LongType),
        None,
        None,
        None))
  }

  test("alter table: update column comment") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c COMMENT 'new comment'"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        Some("new comment"),
        None))
  }

  test("alter table: update column position") {
    comparePlans(
      parsePlan("ALTER TABLE table_name CHANGE COLUMN a.b.c FIRST"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        None,
        None,
        Some(UnresolvedFieldPosition(first()))))
  }

  test("alter table: multiple property changes are not allowed") {
    intercept[ParseException] {
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c " +
        "TYPE bigint COMMENT 'new comment'")}

    intercept[ParseException] {
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c " +
        "TYPE bigint COMMENT AFTER d")}

    intercept[ParseException] {
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c " +
        "TYPE bigint COMMENT 'new comment' AFTER d")}
  }

  test("alter table: SET/DROP NOT NULL") {
    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c SET NOT NULL"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ALTER COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        Some(false),
        None,
        None))

    comparePlans(
      parsePlan("ALTER TABLE table_name ALTER COLUMN a.b.c DROP NOT NULL"),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... ALTER COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        None,
        Some(true),
        None,
        None))
  }

  test("alter table: drop column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name DROP COLUMN a.b.c"),
      DropColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP COLUMNS", None),
        Seq(UnresolvedFieldName(Seq("a", "b", "c")))))
  }

  test("alter table: drop multiple columns") {
    val sql = "ALTER TABLE table_name DROP COLUMN x, y, a.b.c"
    Seq(sql, sql.replace("COLUMN", "COLUMNS")).foreach { drop =>
      comparePlans(
        parsePlan(drop),
        DropColumns(
          UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP COLUMNS", None),
          Seq(UnresolvedFieldName(Seq("x")),
            UnresolvedFieldName(Seq("y")),
            UnresolvedFieldName(Seq("a", "b", "c")))))
    }
  }

  test("alter table: hive style change column") {
    val sql1 = "ALTER TABLE table_name CHANGE COLUMN a.b.c c INT"
    val sql2 = "ALTER TABLE table_name CHANGE COLUMN a.b.c c INT COMMENT 'new_comment'"
    val sql3 = "ALTER TABLE table_name CHANGE COLUMN a.b.c c INT AFTER other_col"

    comparePlans(
      parsePlan(sql1),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(IntegerType),
        None,
        None,
        None))

    comparePlans(
      parsePlan(sql2),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(IntegerType),
        None,
        Some("new_comment"),
        None))

    comparePlans(
      parsePlan(sql3),
      AlterColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... CHANGE COLUMN", None),
        UnresolvedFieldName(Seq("a", "b", "c")),
        Some(IntegerType),
        None,
        None,
        Some(UnresolvedFieldPosition(after("other_col")))))

    // renaming column not supported in hive style ALTER COLUMN.
    intercept("ALTER TABLE table_name CHANGE COLUMN a.b.c new_name INT",
      "please run RENAME COLUMN instead")

    // ALTER COLUMN for a partition is not supported.
    intercept("ALTER TABLE table_name PARTITION (a='1') CHANGE COLUMN a.b.c c INT")
  }

  test("alter table: hive style replace columns") {
    val sql1 = "ALTER TABLE table_name REPLACE COLUMNS (x string)"
    val sql2 = "ALTER TABLE table_name REPLACE COLUMNS (x string COMMENT 'x1')"
    val sql3 = "ALTER TABLE table_name REPLACE COLUMNS (x string COMMENT 'x1', y int)"
    val sql4 = "ALTER TABLE table_name REPLACE COLUMNS (x string COMMENT 'x1', y int COMMENT 'y1')"

    comparePlans(
      parsePlan(sql1),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS", None),
        Seq(QualifiedColType(None, "x", StringType, true, None, None))))

    comparePlans(
      parsePlan(sql2),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS", None),
        Seq(QualifiedColType(None, "x", StringType, true, Some("x1"), None))))

    comparePlans(
      parsePlan(sql3),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS", None),
        Seq(
          QualifiedColType(None, "x", StringType, true, Some("x1"), None),
          QualifiedColType(None, "y", IntegerType, true, None, None)
        )))

    comparePlans(
      parsePlan(sql4),
      ReplaceColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... REPLACE COLUMNS", None),
        Seq(
          QualifiedColType(None, "x", StringType, true, Some("x1"), None),
          QualifiedColType(None, "y", IntegerType, true, Some("y1"), None)
        )))

    intercept("ALTER TABLE table_name PARTITION (a='1') REPLACE COLUMNS (x string)",
      "Operation not allowed: ALTER TABLE table PARTITION partition_spec REPLACE COLUMNS")

    intercept("ALTER TABLE table_name REPLACE COLUMNS (x string NOT NULL)",
      "NOT NULL is not supported in Hive-style REPLACE COLUMNS")

    intercept("ALTER TABLE table_name REPLACE COLUMNS (x string FIRST)",
      "Column position is not supported in Hive-style REPLACE COLUMNS")

    intercept("ALTER TABLE table_name REPLACE COLUMNS (a.b.c string)",
      "Replacing with a nested column is not supported in Hive-style REPLACE COLUMNS")
  }

  test("alter view: rename view") {
    comparePlans(
      parsePlan("ALTER VIEW a.b.c RENAME TO x.y.z"),
      RenameTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ALTER VIEW ... RENAME TO", true),
        Seq("x", "y", "z"),
        isView = true))
  }

  test("describe table column") {
    comparePlans(parsePlan("DESCRIBE t col"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("col")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE t `abc.xyz`"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("abc.xyz")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE t abc.xyz"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("abc", "xyz")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE t `a.b`.`x.y`"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("a.b", "x.y")),
        isExtended = false))

    comparePlans(parsePlan("DESCRIBE TABLE t col"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("col")),
        isExtended = false))
    comparePlans(parsePlan("DESCRIBE TABLE EXTENDED t col"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("col")),
        isExtended = true))
    comparePlans(parsePlan("DESCRIBE TABLE FORMATTED t col"),
      DescribeColumn(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true),
        UnresolvedAttribute(Seq("col")),
        isExtended = true))

    val caught = intercept[AnalysisException](
      parsePlan("DESCRIBE TABLE t PARTITION (ds='1970-01-01') col"))
    assert(caught.getMessage.contains(
        "DESC TABLE COLUMN for a specific partition is not supported"))
  }

  test("SPARK-17328 Fix NPE with EXPLAIN DESCRIBE TABLE") {
    comparePlans(parsePlan("describe t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = false))
    comparePlans(parsePlan("describe table t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = false))
    comparePlans(parsePlan("describe table extended t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = true))
    comparePlans(parsePlan("describe table formatted t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = true))
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
    val exc = intercept[AnalysisException] {
      parsePlan(
        """
          |INSERT OVERWRITE TABLE testcat.ns1.ns2.tbl
          |PARTITION (p1 = 3, p2) IF NOT EXISTS
          |SELECT * FROM source
        """.stripMargin)
    }

    assert(exc.getMessage.contains("IF NOT EXISTS with dynamic partitions"))
    assert(exc.getMessage.contains("p2"))
  }

  test("insert table: if not exists without overwrite fails") {
    val exc = intercept[AnalysisException] {
      parsePlan(
        """
          |INSERT INTO TABLE testcat.ns1.ns2.tbl
          |PARTITION (p1 = 3) IF NOT EXISTS
          |SELECT * FROM source
        """.stripMargin)
    }

    assert(exc.getMessage.contains("INSERT INTO ... IF NOT EXISTS"))
  }

  test("delete from table: delete all") {
    parseCompare("DELETE FROM testcat.ns1.ns2.tbl",
      DeleteFromTable(
        UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl")),
        None))
  }

  test("delete from table: with alias and where clause") {
    parseCompare("DELETE FROM testcat.ns1.ns2.tbl AS t WHERE t.a = 2",
      DeleteFromTable(
        SubqueryAlias("t", UnresolvedRelation(Seq("testcat", "ns1", "ns2", "tbl"))),
        Some(EqualTo(UnresolvedAttribute("t.a"), Literal(2)))))
  }

  test("delete from table: columns aliases is not allowed") {
    val exc = intercept[ParseException] {
      parsePlan("DELETE FROM testcat.ns1.ns2.tbl AS t(a,b,c,d) WHERE d = 2")
    }

    assert(exc.getMessage.contains("Columns aliases are not allowed in DELETE."))
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

  test("update table: columns aliases is not allowed") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |UPDATE testcat.ns1.ns2.tbl AS t(a,b,c,d)
          |SET b='Robert', c=32
          |WHERE d=2
        """.stripMargin)
    }

    assert(exc.getMessage.contains("Columns aliases are not allowed in UPDATE."))
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
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
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
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
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
            Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
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
      """.stripMargin,
    MergeIntoTable(
      SubqueryAlias("target", UnresolvedRelation(Seq("testcat1", "ns1", "ns2", "tbl"))),
      SubqueryAlias("source", UnresolvedRelation(Seq("testcat2", "ns1", "ns2", "tbl"))),
      EqualTo(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
      Seq(UpdateAction(None,
        Seq(Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2"))))),
      Seq(InsertAction(None,
        Seq(Assignment(UnresolvedAttribute("target.col1"), UnresolvedAttribute("source.col1")),
          Assignment(UnresolvedAttribute("target.col2"), UnresolvedAttribute("source.col2")))))))
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
      Seq(InsertStarAction(Some(EqualTo(UnresolvedAttribute("target.col2"), Literal("insert")))))))
  }

  test("merge into table: columns aliases are not allowed") {
    Seq("target(c1, c2)" -> "source", "target" -> "source(c1, c2)").foreach {
      case (targetAlias, sourceAlias) =>
        val exc = intercept[ParseException] {
          parsePlan(
            s"""
              |MERGE INTO testcat1.ns1.ns2.tbl AS $targetAlias
              |USING testcat2.ns1.ns2.tbl AS $sourceAlias
              |ON target.col1 = source.col1
              |WHEN MATCHED AND (target.col2='delete') THEN DELETE
              |WHEN MATCHED AND (target.col2='update') THEN UPDATE SET target.col2 = source.col2
              |WHEN NOT MATCHED AND (target.col2='insert')
              |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
            """.stripMargin)
        }

        assert(exc.getMessage.contains("Columns aliases are not allowed in MERGE."))
    }
  }

  test("merge into table: multi matched and not matched clauses") {
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
              Assignment(UnresolvedAttribute("target.col2"), Literal(2)))))))
  }

  test("merge into table: only the last matched clause can omit the condition") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO testcat1.ns1.ns2.tbl AS target
          |USING testcat2.ns1.ns2.tbl AS source
          |ON target.col1 = source.col1
          |WHEN MATCHED AND (target.col2 == 'update1') THEN UPDATE SET target.col2 = 1
          |WHEN MATCHED THEN UPDATE SET target.col2 = 2
          |WHEN MATCHED THEN DELETE
          |WHEN NOT MATCHED AND (target.col2='insert')
          |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        """.stripMargin)
    }

    assert(exc.getMessage.contains("only the last MATCHED clause can omit the condition"))
  }

  test("merge into table: only the last not matched clause can omit the condition") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO testcat1.ns1.ns2.tbl AS target
          |USING testcat2.ns1.ns2.tbl AS source
          |ON target.col1 = source.col1
          |WHEN MATCHED AND (target.col2 == 'update') THEN UPDATE SET target.col2 = source.col2
          |WHEN MATCHED THEN DELETE
          |WHEN NOT MATCHED AND (target.col2='insert1')
          |THEN INSERT (target.col1, target.col2) values (source.col1, 1)
          |WHEN NOT MATCHED
          |THEN INSERT (target.col1, target.col2) values (source.col1, 2)
          |WHEN NOT MATCHED
          |THEN INSERT (target.col1, target.col2) values (source.col1, source.col2)
        """.stripMargin)
    }

    assert(exc.getMessage.contains("only the last NOT MATCHED clause can omit the condition"))
  }

  test("merge into table: there must be a when (not) matched condition") {
    val exc = intercept[ParseException] {
      parsePlan(
        """
          |MERGE INTO testcat1.ns1.ns2.tbl AS target
          |USING testcat2.ns1.ns2.tbl AS source
          |ON target.col1 = source.col1
        """.stripMargin)
    }

    assert(exc.getMessage.contains("There must be at least one WHEN clause in a MERGE statement"))
  }

  test("show views") {
    comparePlans(
      parsePlan("SHOW VIEWS"),
      ShowViews(UnresolvedNamespace(Seq.empty[String]), None))
    comparePlans(
      parsePlan("SHOW VIEWS '*test*'"),
      ShowViews(UnresolvedNamespace(Seq.empty[String]), Some("*test*")))
    comparePlans(
      parsePlan("SHOW VIEWS LIKE '*test*'"),
      ShowViews(UnresolvedNamespace(Seq.empty[String]), Some("*test*")))
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

  test("create namespace -- backward compatibility with DATABASE/DBPROPERTIES") {
    val expected = CreateNamespace(
      UnresolvedDBObjectName(Seq("a", "b", "c"), true),
      ifNotExists = true,
      Map(
        "a" -> "a",
        "b" -> "b",
        "c" -> "c",
        "comment" -> "namespace_comment",
        "location" -> "/home/user/db"))

    comparePlans(
      parsePlan(
        """
          |CREATE NAMESPACE IF NOT EXISTS a.b.c
          |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
          |COMMENT 'namespace_comment' LOCATION '/home/user/db'
        """.stripMargin),
      expected)

    comparePlans(
      parsePlan(
        """
          |CREATE DATABASE IF NOT EXISTS a.b.c
          |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
          |COMMENT 'namespace_comment' LOCATION '/home/user/db'
        """.stripMargin),
      expected)
  }

  test("create namespace -- check duplicates") {
    def createDatabase(duplicateClause: String): String = {
      s"""
         |CREATE NAMESPACE IF NOT EXISTS a.b.c
         |$duplicateClause
         |$duplicateClause
      """.stripMargin
    }
    val sql1 = createDatabase("COMMENT 'namespace_comment'")
    val sql2 = createDatabase("LOCATION '/home/user/db'")
    val sql3 = createDatabase("WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
    val sql4 = createDatabase("WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")

    intercept(sql1, "Found duplicate clauses: COMMENT")
    intercept(sql2, "Found duplicate clauses: LOCATION")
    intercept(sql3, "Found duplicate clauses: WITH PROPERTIES")
    intercept(sql4, "Found duplicate clauses: WITH DBPROPERTIES")
  }

  test("create namespace - property values must be set") {
    assertUnsupported(
      sql = "CREATE NAMESPACE a.b.c WITH PROPERTIES('key_without_value', 'key_with_value'='x')",
      containsThesePhrases = Seq("key_without_value"))
  }

  test("create namespace -- either PROPERTIES or DBPROPERTIES is allowed") {
    val sql =
      s"""
         |CREATE NAMESPACE IF NOT EXISTS a.b.c
         |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
         |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
      """.stripMargin
    intercept(sql, "Either PROPERTIES or DBPROPERTIES is allowed")
  }

  test("create namespace - support for other types in PROPERTIES") {
    val sql =
      """
        |CREATE NAMESPACE a.b.c
        |LOCATION '/home/user/db'
        |WITH PROPERTIES ('a'=1, 'b'=0.1, 'c'=TRUE)
      """.stripMargin
    comparePlans(
      parsePlan(sql),
      CreateNamespace(
        UnresolvedDBObjectName(Seq("a", "b", "c"), true),
        ifNotExists = false,
        Map(
          "a" -> "1",
          "b" -> "0.1",
          "c" -> "true",
          "location" -> "/home/user/db")))
  }

  test("drop namespace") {
    comparePlans(
      parsePlan("DROP NAMESPACE a.b.c"),
      DropNamespace(
        UnresolvedNamespace(Seq("a", "b", "c")), ifExists = false, cascade = false))

    comparePlans(
      parsePlan("DROP NAMESPACE IF EXISTS a.b.c"),
      DropNamespace(
        UnresolvedNamespace(Seq("a", "b", "c")), ifExists = true, cascade = false))

    comparePlans(
      parsePlan("DROP NAMESPACE IF EXISTS a.b.c RESTRICT"),
      DropNamespace(
        UnresolvedNamespace(Seq("a", "b", "c")), ifExists = true, cascade = false))

    comparePlans(
      parsePlan("DROP NAMESPACE IF EXISTS a.b.c CASCADE"),
      DropNamespace(
        UnresolvedNamespace(Seq("a", "b", "c")), ifExists = true, cascade = true))

    comparePlans(
      parsePlan("DROP NAMESPACE a.b.c CASCADE"),
      DropNamespace(
        UnresolvedNamespace(Seq("a", "b", "c")), ifExists = false, cascade = true))
  }

  test("set namespace properties") {
    comparePlans(
      parsePlan("ALTER DATABASE a.b.c SET PROPERTIES ('a'='a', 'b'='b', 'c'='c')"),
      SetNamespaceProperties(
        UnresolvedNamespace(Seq("a", "b", "c")), Map("a" -> "a", "b" -> "b", "c" -> "c")))

    comparePlans(
      parsePlan("ALTER SCHEMA a.b.c SET PROPERTIES ('a'='a')"),
      SetNamespaceProperties(
        UnresolvedNamespace(Seq("a", "b", "c")), Map("a" -> "a")))

    comparePlans(
      parsePlan("ALTER NAMESPACE a.b.c SET PROPERTIES ('b'='b')"),
      SetNamespaceProperties(
        UnresolvedNamespace(Seq("a", "b", "c")), Map("b" -> "b")))

    comparePlans(
      parsePlan("ALTER DATABASE a.b.c SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')"),
      SetNamespaceProperties(
        UnresolvedNamespace(Seq("a", "b", "c")), Map("a" -> "a", "b" -> "b", "c" -> "c")))

    comparePlans(
      parsePlan("ALTER SCHEMA a.b.c SET DBPROPERTIES ('a'='a')"),
      SetNamespaceProperties(
        UnresolvedNamespace(Seq("a", "b", "c")), Map("a" -> "a")))

    comparePlans(
      parsePlan("ALTER NAMESPACE a.b.c SET DBPROPERTIES ('b'='b')"),
      SetNamespaceProperties(
        UnresolvedNamespace(Seq("a", "b", "c")), Map("b" -> "b")))
  }

  test("set namespace location") {
    comparePlans(
      parsePlan("ALTER DATABASE a.b.c SET LOCATION '/home/user/db'"),
      SetNamespaceLocation(
        UnresolvedNamespace(Seq("a", "b", "c")), "/home/user/db"))

    comparePlans(
      parsePlan("ALTER SCHEMA a.b.c SET LOCATION '/home/user/db'"),
      SetNamespaceLocation(
        UnresolvedNamespace(Seq("a", "b", "c")), "/home/user/db"))

    comparePlans(
      parsePlan("ALTER NAMESPACE a.b.c SET LOCATION '/home/user/db'"),
      SetNamespaceLocation(
        UnresolvedNamespace(Seq("a", "b", "c")), "/home/user/db"))
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

    intercept("analyze table a.b.c compute statistics xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
    intercept("analyze table a.b.c partition (a) compute statistics xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
  }

  test("SPARK-33687: analyze tables statistics") {
    comparePlans(parsePlan("ANALYZE TABLES IN a.b.c COMPUTE STATISTICS"),
      AnalyzeTables(UnresolvedNamespace(Seq("a", "b", "c")), noScan = false))
    comparePlans(parsePlan("ANALYZE TABLES FROM a COMPUTE STATISTICS NOSCAN"),
      AnalyzeTables(UnresolvedNamespace(Seq("a")), noScan = true))
    intercept("ANALYZE TABLES IN a.b.c COMPUTE STATISTICS xxxx",
      "Expected `NOSCAN` instead of `xxxx`")
  }

  test("analyze table column statistics") {
    intercept("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR COLUMNS", "")

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

    intercept("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR ALL COLUMNS key, value",
      "mismatched input 'key' expecting {<EOF>, ';'}")
    intercept("ANALYZE TABLE a.b.c COMPUTE STATISTICS FOR ALL",
      "missing 'COLUMNS' at '<EOF>'")
  }

  test("LOAD DATA INTO table") {
    comparePlans(
      parsePlan("LOAD DATA INPATH 'filepath' INTO TABLE a.b.c"),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA", None),
        "filepath",
        false,
        false,
        None))

    comparePlans(
      parsePlan("LOAD DATA LOCAL INPATH 'filepath' INTO TABLE a.b.c"),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA", None),
        "filepath",
        true,
        false,
        None))

    comparePlans(
      parsePlan("LOAD DATA LOCAL INPATH 'filepath' OVERWRITE INTO TABLE a.b.c"),
      LoadData(
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA", None),
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
        UnresolvedTable(Seq("a", "b", "c"), "LOAD DATA", None),
        "filepath",
        true,
        true,
        Some(Map("ds" -> "2017-06-10"))))
  }

  test("SHOW CREATE table") {
    comparePlans(
      parsePlan("SHOW CREATE TABLE a.b.c"),
      ShowCreateTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "SHOW CREATE TABLE", allowTempView = false)))

    comparePlans(
      parsePlan("SHOW CREATE TABLE a.b.c AS SERDE"),
      ShowCreateTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "SHOW CREATE TABLE", allowTempView = false),
        asSerde = true))
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
        Map("storageLevel" -> "DISK_ONLY")))

    intercept("CACHE TABLE a.b.c AS SELECT * FROM testData",
      "It is not allowed to add catalog/namespace prefix a.b")
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
    assertUnsupported(
      """
        |ALTER VIEW a.b.c ADD IF NOT EXISTS PARTITION
        |(dt='2008-08-08', country='us') PARTITION
        |(dt='2009-09-09', country='uk')
      """.stripMargin)
  }

  test("alter table: SerDe properties") {
    val sql1 = "ALTER TABLE table_name SET SERDE 'org.apache.class'"
    val hint = Some("Please use ALTER VIEW instead.")
    val parsed1 = parsePlan(sql1)
    val expected1 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      None,
      None)
    comparePlans(parsed1, expected1)

    val sql2 =
      """
        |ALTER TABLE table_name SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed2 = parsePlan(sql2)
    val expected2 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed2, expected2)

    val sql3 =
      """
        |ALTER TABLE table_name
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed3 = parsePlan(sql3)
    val expected3 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed3, expected3)

    val sql4 =
      """
        |ALTER TABLE table_name PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed4 = parsePlan(sql4)
    val expected4 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed4, expected4)

    val sql5 =
      """
        |ALTER TABLE table_name PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed5 = parsePlan(sql5)
    val expected5 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed5, expected5)

    val sql6 =
      """
        |ALTER TABLE a.b.c SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed6 = parsePlan(sql6)
    val expected6 = SetTableSerDeProperties(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed6, expected6)

    val sql7 =
      """
        |ALTER TABLE a.b.c PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed7 = parsePlan(sql7)
    val expected7 = SetTableSerDeProperties(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed7, expected7)
  }

  test("alter view: AS Query") {
    val parsed = parsePlan("ALTER VIEW a.b.c AS SELECT 1")
    val expected = AlterViewAs(
      UnresolvedView(Seq("a", "b", "c"), "ALTER VIEW ... AS", true, None),
      "SELECT 1",
      parsePlan("SELECT 1"))
    comparePlans(parsed, expected)
  }

  test("DESCRIBE FUNCTION") {
    comparePlans(
      parsePlan("DESC FUNCTION a"),
      DescribeFunction(UnresolvedFunc(Seq("a")), false))
    comparePlans(
      parsePlan("DESCRIBE FUNCTION a"),
      DescribeFunction(UnresolvedFunc(Seq("a")), false))
    comparePlans(
      parsePlan("DESCRIBE FUNCTION a.b.c"),
      DescribeFunction(UnresolvedFunc(Seq("a", "b", "c")), false))
    comparePlans(
      parsePlan("DESCRIBE FUNCTION EXTENDED a.b.c"),
      DescribeFunction(UnresolvedFunc(Seq("a", "b", "c")), true))
  }

  test("SHOW FUNCTIONS") {
    comparePlans(
      parsePlan("SHOW FUNCTIONS"),
      ShowFunctions(None, true, true, None))
    comparePlans(
      parsePlan("SHOW USER FUNCTIONS"),
      ShowFunctions(None, true, false, None))
    comparePlans(
      parsePlan("SHOW user FUNCTIONS"),
      ShowFunctions(None, true, false, None))
    comparePlans(
      parsePlan("SHOW SYSTEM FUNCTIONS"),
      ShowFunctions(None, false, true, None))
    comparePlans(
      parsePlan("SHOW ALL FUNCTIONS"),
      ShowFunctions(None, true, true, None))
    comparePlans(
      parsePlan("SHOW FUNCTIONS LIKE 'funct*'"),
      ShowFunctions(None, true, true, Some("funct*")))
    comparePlans(
      parsePlan("SHOW FUNCTIONS LIKE a.b.c"),
      ShowFunctions(Some(UnresolvedFunc(Seq("a", "b", "c"))), true, true, None))
    val sql = "SHOW other FUNCTIONS"
    intercept(sql, s"$sql not supported")
  }

  test("DROP FUNCTION") {
    comparePlans(
      parsePlan("DROP FUNCTION a"),
      DropFunction(UnresolvedFunc(Seq("a")), false, false))
    comparePlans(
      parsePlan("DROP FUNCTION a.b.c"),
      DropFunction(UnresolvedFunc(Seq("a", "b", "c")), false, false))
    comparePlans(
      parsePlan("DROP TEMPORARY FUNCTION a.b.c"),
      DropFunction(UnresolvedFunc(Seq("a", "b", "c")), false, true))
    comparePlans(
      parsePlan("DROP FUNCTION IF EXISTS a.b.c"),
      DropFunction(UnresolvedFunc(Seq("a", "b", "c")), true, false))
    comparePlans(
      parsePlan("DROP TEMPORARY FUNCTION IF EXISTS a.b.c"),
      DropFunction(UnresolvedFunc(Seq("a", "b", "c")), true, true))
  }

  test("REFRESH FUNCTION") {
    parseCompare("REFRESH FUNCTION c",
      RefreshFunction(UnresolvedFunc(Seq("c"))))
    parseCompare("REFRESH FUNCTION b.c",
      RefreshFunction(UnresolvedFunc(Seq("b", "c"))))
    parseCompare("REFRESH FUNCTION a.b.c",
      RefreshFunction(UnresolvedFunc(Seq("a", "b", "c"))))
  }

  test("CREATE INDEX") {
    parseCompare("CREATE index i1 ON a.b.c USING BTREE (col1)",
      CreateIndex(UnresolvedTable(Seq("a", "b", "c"), "CREATE INDEX", None), "i1", "BTREE", false,
        Seq(UnresolvedFieldName(Seq("col1"))).zip(Seq(Map.empty[String, String])), Map.empty))

    parseCompare("CREATE index IF NOT EXISTS i1 ON TABLE a.b.c USING BTREE" +
      " (col1 OPTIONS ('k1'='v1'), col2 OPTIONS ('k2'='v2')) ",
      CreateIndex(UnresolvedTable(Seq("a", "b", "c"), "CREATE INDEX", None), "i1", "BTREE", true,
        Seq(UnresolvedFieldName(Seq("col1")), UnresolvedFieldName(Seq("col2")))
          .zip(Seq(Map("k1" -> "v1"), Map("k2" -> "v2"))), Map.empty))

    parseCompare("CREATE index i1 ON a.b.c" +
      " (col1 OPTIONS ('k1'='v1'), col2 OPTIONS ('k2'='v2')) OPTIONS ('k3'='v3', 'k4'='v4')",
      CreateIndex(UnresolvedTable(Seq("a", "b", "c"), "CREATE INDEX", None), "i1", "", false,
        Seq(UnresolvedFieldName(Seq("col1")), UnresolvedFieldName(Seq("col2")))
          .zip(Seq(Map("k1" -> "v1"), Map("k2" -> "v2"))), Map("k3" -> "v3", "k4" -> "v4")))
  }

  test("DROP INDEX") {
    parseCompare("DROP index i1 ON a.b.c",
      DropIndex(UnresolvedTable(Seq("a", "b", "c"), "DROP INDEX", None), "i1", false))

    parseCompare("DROP index IF EXISTS i1 ON a.b.c",
      DropIndex(UnresolvedTable(Seq("a", "b", "c"), "DROP INDEX", None), "i1", true))
  }

  private case class TableSpec(
      name: Seq[String],
      schema: Option[StructType],
      partitioning: Seq[Transform],
      bucketSpec: Option[BucketSpec],
      properties: Map[String, String],
      provider: Option[String],
      options: Map[String, String],
      location: Option[String],
      comment: Option[String],
      serdeInfo: Option[SerdeInfo],
      external: Boolean = false)

  private object TableSpec {
    def apply(plan: LogicalPlan): TableSpec = {
      plan match {
        case create: CreateTableStatement =>
          TableSpec(
            create.tableName,
            Some(create.tableSchema),
            create.partitioning,
            create.bucketSpec,
            create.properties,
            create.provider,
            create.options,
            create.location,
            create.comment,
            create.serde,
            create.external)
        case replace: ReplaceTableStatement =>
          TableSpec(
            replace.tableName,
            Some(replace.tableSchema),
            replace.partitioning,
            replace.bucketSpec,
            replace.properties,
            replace.provider,
            replace.options,
            replace.location,
            replace.comment,
            replace.serde)
        case ctas: CreateTableAsSelectStatement =>
          TableSpec(
            ctas.tableName,
            Some(ctas.asSelect).filter(_.resolved).map(_.schema),
            ctas.partitioning,
            ctas.bucketSpec,
            ctas.properties,
            ctas.provider,
            ctas.options,
            ctas.location,
            ctas.comment,
            ctas.serde,
            ctas.external)
        case rtas: ReplaceTableAsSelectStatement =>
          TableSpec(
            rtas.tableName,
            Some(rtas.asSelect).filter(_.resolved).map(_.schema),
            rtas.partitioning,
            rtas.bucketSpec,
            rtas.properties,
            rtas.provider,
            rtas.options,
            rtas.location,
            rtas.comment,
            rtas.serde)
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
      CommentOnTable(UnresolvedTable(Seq("a", "b", "c"), "COMMENT ON TABLE", None), "xYz"))
  }

  test("create table - without using") {
    val sql = "CREATE TABLE 1m.2g(a INT)"
    val expectedTableSpec = TableSpec(
      Seq("1m", "2g"),
      Some(new StructType().add("a", IntegerType)),
      Seq.empty[Transform],
      None,
      Map.empty[String, String],
      None,
      Map.empty[String, String],
      None,
      None,
      None)

    testCreateOrReplaceDdl(sql, expectedTableSpec, expectedIfNotExists = false)
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    def insertPartitionPlan(part: String): InsertIntoStatement = {
      InsertIntoStatement(
        UnresolvedRelation(Seq("t")),
        Map("part" -> Some(part)),
        Seq.empty[String],
        UnresolvedInlineTable(Seq("col1"), Seq(Seq(Literal("a")))),
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

    comparePlans(parsePlan(dateTypeSql), insertPartitionPlan("2019-01-02"))
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      comparePlans(parsePlan(intervalTypeSql), insertPartitionPlan(interval))
    }
    comparePlans(parsePlan(ymIntervalTypeSql), insertPartitionPlan("INTERVAL '1-2' YEAR TO MONTH"))
    comparePlans(parsePlan(dtIntervalTypeSql),
      insertPartitionPlan("INTERVAL '1 02:03:04.128462' DAY TO SECOND"))
    comparePlans(parsePlan(timestampTypeSql), insertPartitionPlan(timestamp))
    comparePlans(parsePlan(binaryTypeSql), insertPartitionPlan(binaryStr))
  }

  test("as of syntax") {
    val properties = new util.HashMap[String, String]
    var timeTravel = TimeTravelSpec.create(None, Some("Snapshot123456789"))
    comparePlans(
      parsePlan("SELECT * FROM a.b.c VERSION AS OF 'Snapshot123456789'"),
      Project(Seq(UnresolvedStar(None)),
        UnresolvedRelation(
          Seq("a", "b", "c"),
          new CaseInsensitiveStringMap(properties),
          timeTravelSpec = timeTravel)))

    timeTravel = TimeTravelSpec.create(None, Some("123456789"))
    comparePlans(
      parsePlan("SELECT * FROM a.b.c FOR SYSTEM_VERSION AS OF 123456789"),
      Project(Seq(UnresolvedStar(None)),
        UnresolvedRelation(
          Seq("a", "b", "c"),
          new CaseInsensitiveStringMap(properties),
          timeTravelSpec = timeTravel)))

    timeTravel = TimeTravelSpec.create(Some("2019-01-29 00:37:58"), None)
    comparePlans(
      parsePlan("SELECT * FROM a.b.c TIMESTAMP AS OF '2019-01-29 00:37:58'"),
      Project(Seq(UnresolvedStar(None)),
        UnresolvedRelation(
          Seq("a", "b", "c"),
          new CaseInsensitiveStringMap(properties),
          timeTravelSpec = timeTravel)))
    comparePlans(
      parsePlan("SELECT * FROM a.b.c FOR SYSTEM_TIME AS OF '2019-01-29 00:37:58'"),
      Project(Seq(UnresolvedStar(None)),
        UnresolvedRelation(
          Seq("a", "b", "c"),
          new CaseInsensitiveStringMap(properties),
          timeTravelSpec = timeTravel)))

    timeTravel = TimeTravelSpec.create(Some("2019-01-29"), None)
    comparePlans(
      parsePlan("SELECT * FROM a.b.c TIMESTAMP AS OF '2019-01-29'"),
      Project(Seq(UnresolvedStar(None)),
        UnresolvedRelation(
          Seq("a", "b", "c"),
          new CaseInsensitiveStringMap(properties),
          timeTravelSpec = timeTravel)))
    comparePlans(
      parsePlan("SELECT * FROM a.b.c FOR SYSTEM_TIME AS OF '2019-01-29'"),
      Project(Seq(UnresolvedStar(None)),
        UnresolvedRelation(
          Seq("a", "b", "c"),
          new CaseInsensitiveStringMap(properties),
          timeTravelSpec = timeTravel)))

    val e1 = intercept[DateTimeException] {
      parsePlan("SELECT * FROM a.b.c TIMESTAMP AS OF '2019-01-11111'")
    }.getMessage
    assert(e1.contains("Cannot cast 2019-01-11111 to TimestampType."))

    val e2 = intercept[AnalysisException] {
      timeTravel = TimeTravelSpec.create(Some("2019-01-29 00:37:58"), Some("123456789"))
    }.getMessage
    assert(e2.contains("Cannot specify both version and timestamp when scanning the table."))
  }
}
