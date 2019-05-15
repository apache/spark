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

import org.apache.spark.sql.catalog.v2.expressions.{ApplyTransform, BucketTransform, DaysTransform, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, YearsTransform}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.sql.{CreateTableAsSelectStatement, CreateTableStatement}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class DDLParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  private def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
  }

  test("create table using - schema") {
    val sql = "CREATE TABLE my_tab(a INT COMMENT 'test', b STRING) USING parquet"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType()
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }

    intercept("CREATE TABLE my_tab(a: INT COMMENT 'test', b: STRING) USING parquet",
      "no viable alternative at input")
  }

  test("create table - with IF NOT EXISTS") {
    val sql = "CREATE TABLE IF NOT EXISTS my_tab(a INT, b STRING) USING parquet"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with partitioned by") {
    val query = "CREATE TABLE my_tab(a INT comment 'test', b STRING) " +
        "USING parquet PARTITIONED BY (a)"

    parsePlan(query) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType()
            .add("a", IntegerType, nullable = true, "test")
            .add("b", StringType))
        assert(create.partitioning == Seq(IdentityTransform(FieldReference("a"))))
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - partitioned by transforms") {
    val sql =
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

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType()
            .add("a", IntegerType)
            .add("b", StringType)
            .add("ts", TimestampType))
        assert(create.partitioning == Seq(
            IdentityTransform(FieldReference("a")),
            BucketTransform(LiteralValue(16, IntegerType), Seq(FieldReference("b"))),
            YearsTransform(FieldReference("ts")),
            MonthsTransform(FieldReference("ts")),
            DaysTransform(FieldReference("ts")),
            HoursTransform(FieldReference("ts")),
            ApplyTransform("foo", Seq(
                FieldReference("a"),
                LiteralValue(UTF8String.fromString("bar"), StringType),
                LiteralValue(34, IntegerType)))))
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with bucket") {
    val query = "CREATE TABLE my_tab(a INT, b STRING) USING parquet " +
        "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS"

    parsePlan(query) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.contains(BucketSpec(5, Seq("a"), Seq("b"))))
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $query")
    }
  }

  test("create table - with comment") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet COMMENT 'abc'"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.contains("abc"))
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with table properties") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet TBLPROPERTIES('test' = 'test')"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties == Map("test" -> "test"))
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - with location") {
    val sql = "CREATE TABLE my_tab(a INT, b STRING) USING parquet LOCATION '/tmp/file'"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("my_tab"))
        assert(create.tableSchema == new StructType().add("a", IntegerType).add("b", StringType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.contains("/tmp/file"))
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("create table - byte length literal table name") {
    val sql = "CREATE TABLE 1m.2g(a INT) USING parquet"

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("1m", "2g"))
        assert(create.tableSchema == new StructType().add("a", IntegerType))
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "parquet")
        assert(create.options.isEmpty)
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
    }
  }

  test("Duplicate clauses - create table") {
    def createTableHeader(duplicateClause: String): String = {
      s"CREATE TABLE my_tab(a INT, b STRING) USING parquet $duplicateClause $duplicateClause"
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
  }

  test("support for other types in OPTIONS") {
    val sql =
      """
        |CREATE TABLE table_name USING json
        |OPTIONS (a 1, b 0.1, c TRUE)
      """.stripMargin

    parsePlan(sql) match {
      case create: CreateTableStatement =>
        assert(create.tableName == Seq("table_name"))
        assert(create.tableSchema == new StructType)
        assert(create.partitioning.isEmpty)
        assert(create.bucketSpec.isEmpty)
        assert(create.properties.isEmpty)
        assert(create.provider == "json")
        assert(create.options == Map("a" -> "1", "b" -> "0.1", "c" -> "true"))
        assert(create.location.isEmpty)
        assert(create.comment.isEmpty)
        assert(!create.ifNotExists)

      case other =>
        fail(s"Expected to parse ${classOf[CreateTableStatement].getClass.getName} from query," +
            s"got ${other.getClass.getName}: $sql")
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

    checkParsing(s1)
    checkParsing(s2)
    checkParsing(s3)

    def checkParsing(sql: String): Unit = {
      parsePlan(sql) match {
        case create: CreateTableAsSelectStatement =>
          assert(create.tableName == Seq("mydb", "page_view"))
          assert(create.partitioning.isEmpty)
          assert(create.bucketSpec.isEmpty)
          assert(create.properties == Map("p1" -> "v1", "p2" -> "v2"))
          assert(create.provider == "parquet")
          assert(create.options.isEmpty)
          assert(create.location.contains("/user/external/page_view"))
          assert(create.comment.contains("This is the staging page view table"))
          assert(create.ifNotExists)

        case other =>
          fail(s"Expected to parse ${classOf[CreateTableAsSelectStatement].getClass.getName} " +
              s"from query, got ${other.getClass.getName}: $sql")
      }
    }
  }
}
