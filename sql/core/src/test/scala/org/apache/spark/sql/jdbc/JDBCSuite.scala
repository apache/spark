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

package org.apache.spark.sql.jdbc

import java.math.BigDecimal
import java.sql.{Date, DriverManager, SQLException, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.{Calendar, GregorianCalendar, Properties}

import scala.collection.JavaConverters._

import org.h2.jdbc.JdbcSQLException
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeTestUtils}
import org.apache.spark.sql.execution.{DataSourceScanExec, ExtendedMode}
import org.apache.spark.sql.execution.command.{ExplainCommand, ShowCreateTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRDD, JDBCRelation, JdbcUtils}
import org.apache.spark.sql.execution.metric.InputOutputMetricsHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCSuite extends QueryTest
  with BeforeAndAfter with PrivateMethodTester with SharedSparkSession {
  import testImplicits._

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")
    override def getCatalystType(
        sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
      Some(StringType)
  }

  val testH2DialectTinyInt = new JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")
    override def getCatalystType(
        sqlType: Int,
        typeName: String,
        size: Int,
        md: MetadataBuilder): Option[DataType] = {
      sqlType match {
        case java.sql.Types.TINYINT => Some(ByteType)
        case _ => None
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    // Extra properties that will be specified for our database. We need these to test
    // usage of parameters from OPTIONS clause in queries.
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement(
      "insert into test.people values ('joe ''foo'' \"bar\"', 3)").executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW foobar
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW fetchtwo
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         ${JDBCOptions.JDBC_BATCH_FETCH_SIZE} '2')
       """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW parts
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         partitionColumn 'THEID', lowerBound '1', upperBound '4', numPartitions '3')
       """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW partsoverflow
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         partitionColumn 'THEID', lowerBound '-9223372036854775808',
        |         upperBound '9223372036854775807', numPartitions '3')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.inttypes (a INT, b BOOLEAN, c TINYINT, "
      + "d SMALLINT, e BIGINT)").executeUpdate()
    conn.prepareStatement("insert into test.inttypes values (1, false, 3, 4, 1234567890123)"
        ).executeUpdate()
    conn.prepareStatement("insert into test.inttypes values (null, null, null, null, null)"
        ).executeUpdate()
    conn.commit()
    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW inttypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.INTTYPES', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.strtypes (a BINARY(20), b VARCHAR(20), "
      + "c VARCHAR_IGNORECASE(20), d CHAR(20), e BLOB, f CLOB)").executeUpdate()
    val stmt = conn.prepareStatement("insert into test.strtypes values (?, ?, ?, ?, ?, ?)")
    stmt.setBytes(1, testBytes)
    stmt.setString(2, "Sensitive")
    stmt.setString(3, "Insensitive")
    stmt.setString(4, "Twenty-byte CHAR")
    stmt.setBytes(5, testBytes)
    stmt.setString(6, "I am a clob!")
    stmt.executeUpdate()
    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW strtypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.STRTYPES', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.timetypes (a TIME, b DATE, c TIMESTAMP)"
        ).executeUpdate()
    conn.prepareStatement("insert into test.timetypes values ('12:34:56', "
      + "'1996-01-01', '2002-02-20 11:22:33.543543543')").executeUpdate()
    conn.prepareStatement("insert into test.timetypes values ('12:34:56', "
      + "null, '2002-02-20 11:22:33.543543543')").executeUpdate()
    conn.commit()
    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW timetypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.TIMETYPES', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("CREATE TABLE test.timezone (tz TIMESTAMP WITH TIME ZONE) " +
      "AS SELECT '1999-01-08 04:05:06.543543543 GMT-08:00'")
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("CREATE TABLE test.array (ar ARRAY) " +
      "AS SELECT '(1, 2, 3)'")
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("create table test.flttypes (a DOUBLE, b REAL, c DECIMAL(38, 18))"
        ).executeUpdate()
    conn.prepareStatement("insert into test.flttypes values ("
      + "1.0000000000000002220446049250313080847263336181640625, "
      + "1.00000011920928955078125, "
      + "123456789012345.543215432154321)").executeUpdate()
    conn.commit()
    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW flttypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.FLTTYPES', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement(
      s"""
        |create table test.nulltypes (a INT, b BOOLEAN, c TINYINT, d BINARY(20), e VARCHAR(20),
        |f VARCHAR_IGNORECASE(20), g CHAR(20), h BLOB, i CLOB, j TIME, k DATE, l TIMESTAMP,
        |m DOUBLE, n REAL, o DECIMAL(38, 18))
       """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.prepareStatement("insert into test.nulltypes values ("
      + "null, null, null, null, null, null, null, null, null, "
      + "null, null, null, null, null, null)").executeUpdate()
    conn.commit()
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW nulltypes
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST.NULLTYPES', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement(
      "create table test.emp(name TEXT(32) NOT NULL," +
        " theid INTEGER, \"Dept\" INTEGER)").executeUpdate()
    conn.prepareStatement(
      "insert into test.emp values ('fred', 1, 10)").executeUpdate()
    conn.prepareStatement(
      "insert into test.emp values ('mary', 2, null)").executeUpdate()
    conn.prepareStatement(
      "insert into test.emp values ('joe ''foo'' \"bar\"', 3, 30)").executeUpdate()
    conn.prepareStatement(
      "insert into test.emp values ('kathy', null, null)").executeUpdate()
    conn.commit()

    conn.prepareStatement(
      "create table test.seq(id INTEGER)").executeUpdate()
    (0 to 6).foreach { value =>
      conn.prepareStatement(
        s"insert into test.seq values ($value)").executeUpdate()
    }
    conn.prepareStatement(
      "insert into test.seq values (null)").executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW nullparts
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.EMP', user 'testUser', password 'testPass',
        |partitionColumn '"Dept"', lowerBound '1', upperBound '4', numPartitions '3')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement(
      """create table test."mixedCaseCols" ("Name" TEXT(32), "Id" INTEGER NOT NULL)""")
      .executeUpdate()
    conn.prepareStatement("""insert into test."mixedCaseCols" values ('fred', 1)""").executeUpdate()
    conn.prepareStatement("""insert into test."mixedCaseCols" values ('mary', 2)""").executeUpdate()
    conn.prepareStatement("""insert into test."mixedCaseCols" values (null, 3)""").executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW mixedCaseCols
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST."mixedCaseCols"', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("CREATE TABLE test.partition (THEID INTEGER, `THE ID` INTEGER) " +
      "AS SELECT 1, 1")
      .executeUpdate()
    conn.commit()

    conn.prepareStatement("CREATE TABLE test.datetime (d DATE, t TIMESTAMP)").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO test.datetime VALUES ('2018-07-06', '2018-07-06 05:50:00.0')").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO test.datetime VALUES ('2018-07-06', '2018-07-06 08:10:08.0')").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO test.datetime VALUES ('2018-07-08', '2018-07-08 13:32:01.0')").executeUpdate()
    conn.prepareStatement(
      "INSERT INTO test.datetime VALUES ('2018-07-12', '2018-07-12 09:51:15.0')").executeUpdate()
    conn.commit()

    // Untested: IDENTITY, OTHER, UUID, ARRAY, and GEOMETRY types.
  }

  override def afterAll(): Unit = {
    conn.close()
    super.afterAll()
  }

  // Check whether the tables are fetched in the expected degree of parallelism
  def checkNumPartitions(df: DataFrame, expectedNumPartitions: Int): Unit = {
    val jdbcRelations = df.queryExecution.analyzed.collect {
      case LogicalRelation(r: JDBCRelation, _, _, _) => r
    }
    assert(jdbcRelations.length == 1)
    assert(jdbcRelations.head.parts.length == expectedNumPartitions,
      s"Expecting a JDBCRelation with $expectedNumPartitions partitions, but got:`$jdbcRelations`")
  }

  private def checkPushdown(df: DataFrame): DataFrame = {
    val parentPlan = df.queryExecution.executedPlan
    // Check if SparkPlan Filter is removed in a physical plan and
    // the plan only has PhysicalRDD to scan JDBCRelation.
    assert(parentPlan.isInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec])
    val node = parentPlan.asInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec]
    assert(node.child.isInstanceOf[org.apache.spark.sql.execution.DataSourceScanExec])
    assert(node.child.asInstanceOf[DataSourceScanExec].nodeName.contains("JDBCRelation"))
    df
  }

  private def checkNotPushdown(df: DataFrame): DataFrame = {
    val parentPlan = df.queryExecution.executedPlan
    // Check if SparkPlan Filter is not removed in a physical plan because JDBCRDD
    // cannot compile given predicates.
    assert(parentPlan.isInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec])
    val node = parentPlan.asInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec]
    assert(node.child.isInstanceOf[org.apache.spark.sql.execution.FilterExec])
    df
  }

  test("SELECT *") {
    assert(sql("SELECT * FROM foobar").collect().size === 3)
  }

  test("SELECT * WHERE (simple predicates)") {
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE THEID < 1")).collect().size == 0)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE THEID != 2")).collect().size == 2)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE THEID = 1")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME = 'fred'")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME <=> 'fred'")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME > 'fred'")).collect().size == 2)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME != 'fred'")).collect().size == 2)

    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME IN ('mary', 'fred')"))
      .collect().size == 2)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME NOT IN ('fred')"))
      .collect().size == 2)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE THEID = 1 OR NAME = 'mary'"))
      .collect().size == 2)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE THEID = 1 OR NAME = 'mary' "
      + "AND THEID = 2")).collect().size == 2)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME LIKE 'fr%'")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME LIKE '%ed'")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM foobar WHERE NAME LIKE '%re%'")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM nulltypes WHERE A IS NULL")).collect().size == 1)
    assert(checkPushdown(sql("SELECT * FROM nulltypes WHERE A IS NOT NULL")).collect().size == 0)

    // This is a test to reflect discussion in SPARK-12218.
    // The older versions of spark have this kind of bugs in parquet data source.
    val df1 = sql("SELECT * FROM foobar WHERE NOT (THEID != 2) OR NOT (NAME != 'mary')")
    assert(df1.collect.toSet === Set(Row("mary", 2)))

    // SPARK-22548: Incorrect nested AND expression pushed down to JDBC data source
    val df2 = sql("SELECT * FROM foobar " +
      "WHERE (THEID > 0 AND TRIM(NAME) = 'mary') OR (NAME = 'fred')")
    assert(df2.collect.toSet === Set(Row("fred", 1), Row("mary", 2)))

    assert(checkNotPushdown(sql("SELECT * FROM foobar WHERE (THEID + 1) < 2")).collect().size == 0)
    assert(checkNotPushdown(sql("SELECT * FROM foobar WHERE (THEID + 2) != 4")).collect().size == 2)
  }

  test("SELECT COUNT(1) WHERE (predicates)") {
    // Check if an answer is correct when Filter is removed from operations such as count() which
    // does not require any columns. In some data sources, e.g., Parquet, `requiredColumns` in
    // org.apache.spark.sql.sources.interfaces is not given in logical plans, but some filters
    // are applied for columns with Filter producing wrong results. On the other hand, JDBCRDD
    // correctly handles this case by assigning `requiredColumns` properly. See PR 10427 for more
    // discussions.
    assert(sql("SELECT COUNT(1) FROM foobar WHERE NAME = 'mary'").collect.toSet === Set(Row(1)))
  }

  test("SELECT * WHERE (quoted strings)") {
    assert(sql("select * from foobar").where('NAME === "joe 'foo' \"bar\"").collect().size === 1)
  }

  test("SELECT first field") {
    val names = sql("SELECT NAME FROM foobar").collect().map(x => x.getString(0)).sortWith(_ < _)
    assert(names.size === 3)
    assert(names(0).equals("fred"))
    assert(names(1).equals("joe 'foo' \"bar\""))
    assert(names(2).equals("mary"))
  }

  test("SELECT first field when fetchsize is two") {
    val names = sql("SELECT NAME FROM fetchtwo").collect().map(x => x.getString(0)).sortWith(_ < _)
    assert(names.size === 3)
    assert(names(0).equals("fred"))
    assert(names(1).equals("joe 'foo' \"bar\""))
    assert(names(2).equals("mary"))
  }

  test("SELECT second field") {
    val ids = sql("SELECT THEID FROM foobar").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }

  test("SELECT second field when fetchsize is two") {
    val ids = sql("SELECT THEID FROM fetchtwo").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }

  test("SELECT * partitioned") {
    val df = sql("SELECT * FROM parts")
    checkNumPartitions(df, expectedNumPartitions = 3)
    assert(df.collect().length == 3)
  }

  test("SELECT WHERE (simple predicates) partitioned") {
    val df1 = sql("SELECT * FROM parts WHERE THEID < 1")
    checkNumPartitions(df1, expectedNumPartitions = 3)
    assert(df1.collect().length === 0)

    val df2 = sql("SELECT * FROM parts WHERE THEID != 2")
    checkNumPartitions(df2, expectedNumPartitions = 3)
    assert(df2.collect().length === 2)

    val df3 = sql("SELECT THEID FROM parts WHERE THEID = 1")
    checkNumPartitions(df3, expectedNumPartitions = 3)
    assert(df3.collect().length === 1)
  }

  test("SELECT second field partitioned") {
    val ids = sql("SELECT THEID FROM parts").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }

  test("overflow of partition bound difference does not give negative stride") {
    val df = sql("SELECT * FROM partsoverflow")
    checkNumPartitions(df, expectedNumPartitions = 3)
    assert(df.collect().length == 3)
  }

  test("Register JDBC query with renamed fields") {
    // Regression test for bug SPARK-7345
    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW renamed
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable '(select NAME as NAME1, NAME as NAME2 from TEST.PEOPLE)',
        |user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    val df = sql("SELECT * FROM renamed")
    assert(df.schema.fields.size == 2)
    assert(df.schema.fields(0).name == "NAME1")
    assert(df.schema.fields(1).name == "NAME2")
  }

  test("Basic API") {
    assert(spark.read.jdbc(
      urlWithUserAndPass, "TEST.PEOPLE", new Properties()).collect().length === 3)
  }

  test("Missing partition columns") {
    withView("tempPeople") {
      val e = intercept[IllegalArgumentException] {
        sql(
          s"""
             |CREATE OR REPLACE TEMPORARY VIEW tempPeople
             |USING org.apache.spark.sql.jdbc
             |OPTIONS (
             |  url 'jdbc:h2:mem:testdb0;user=testUser;password=testPass',
             |  dbtable 'TEST.PEOPLE',
             |  lowerBound '0',
             |  upperBound '52',
             |  numPartitions '53',
             |  fetchSize '10000' )
           """.stripMargin.replaceAll("\n", " "))
      }.getMessage
      assert(e.contains("When reading JDBC data sources, users need to specify all or none " +
        "for the following options: 'partitionColumn', 'lowerBound', 'upperBound', and " +
        "'numPartitions'"))
    }
  }

  test("Basic API with FetchSize") {
    (0 to 4).foreach { size =>
      val properties = new Properties()
      properties.setProperty(JDBCOptions.JDBC_BATCH_FETCH_SIZE, size.toString)
      assert(spark.read.jdbc(
        urlWithUserAndPass, "TEST.PEOPLE", properties).collect().length === 3)
    }
  }

  test("Partitioning via JDBCPartitioningInfo API") {
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", "THEID", 0, 4, 3, new Properties())
    checkNumPartitions(df, expectedNumPartitions = 3)
    assert(df.collect().length === 3)
  }

  test("Partitioning via list-of-where-clauses API") {
    val parts = Array[String]("THEID < 2", "THEID >= 2")
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", parts, new Properties())
    checkNumPartitions(df, expectedNumPartitions = 2)
    assert(df.collect().length === 3)
  }

  test("Partitioning on column that might have null values.") {
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.EMP", "theid", 0, 4, 3, new Properties())
    checkNumPartitions(df, expectedNumPartitions = 3)
    assert(df.collect().length === 4)

    val df2 = spark.read.jdbc(urlWithUserAndPass, "TEST.EMP", "THEID", 0, 4, 3, new Properties())
    checkNumPartitions(df2, expectedNumPartitions = 3)
    assert(df2.collect().length === 4)

    // partitioning on a nullable quoted column
    assert(
      spark.read.jdbc(urlWithUserAndPass, "TEST.EMP", """"Dept"""", 0, 4, 3, new Properties())
        .collect().length === 4)
  }

  test("Partitioning on column where numPartitions is zero") {
    val res = spark.read.jdbc(
      url = urlWithUserAndPass,
      table = "TEST.seq",
      columnName = "id",
      lowerBound = 0,
      upperBound = 4,
      numPartitions = 0,
      connectionProperties = new Properties()
    )
    checkNumPartitions(res, expectedNumPartitions = 1)
    assert(res.count() === 8)
  }

  test("Partitioning on column where numPartitions are more than the number of total rows") {
    val res = spark.read.jdbc(
      url = urlWithUserAndPass,
      table = "TEST.seq",
      columnName = "id",
      lowerBound = 1,
      upperBound = 5,
      numPartitions = 10,
      connectionProperties = new Properties()
    )
    checkNumPartitions(res, expectedNumPartitions = 4)
    assert(res.count() === 8)
  }

  test("Partitioning on column where lowerBound is equal to upperBound") {
    val res = spark.read.jdbc(
      url = urlWithUserAndPass,
      table = "TEST.seq",
      columnName = "id",
      lowerBound = 5,
      upperBound = 5,
      numPartitions = 4,
      connectionProperties = new Properties()
    )
    checkNumPartitions(res, expectedNumPartitions = 1)
    assert(res.count() === 8)
  }

  test("Partitioning on column where lowerBound is larger than upperBound") {
    val e = intercept[IllegalArgumentException] {
      spark.read.jdbc(
        url = urlWithUserAndPass,
        table = "TEST.seq",
        columnName = "id",
        lowerBound = 5,
        upperBound = 1,
        numPartitions = 3,
        connectionProperties = new Properties()
      )
    }.getMessage
    assert(e.contains("Operation not allowed: the lower bound of partitioning column " +
      "is larger than the upper bound. Lower bound: 5; Upper bound: 1"))
  }

  test("SELECT * on partitioned table with a nullable partition column") {
    val df = sql("SELECT * FROM nullparts")
    checkNumPartitions(df, expectedNumPartitions = 3)
    assert(df.collect().length == 4)
  }

  test("H2 integral types") {
    val rows = sql("SELECT * FROM inttypes WHERE A IS NOT NULL").collect()
    assert(rows.length === 1)
    assert(rows(0).getInt(0) === 1)
    assert(rows(0).getBoolean(1) === false)
    assert(rows(0).getInt(2) === 3)
    assert(rows(0).getInt(3) === 4)
    assert(rows(0).getLong(4) === 1234567890123L)
  }

  test("H2 null entries") {
    val rows = sql("SELECT * FROM inttypes WHERE A IS NULL").collect()
    assert(rows.length === 1)
    assert(rows(0).isNullAt(0))
    assert(rows(0).isNullAt(1))
    assert(rows(0).isNullAt(2))
    assert(rows(0).isNullAt(3))
    assert(rows(0).isNullAt(4))
  }

  test("H2 string types") {
    val rows = sql("SELECT * FROM strtypes").collect()
    assert(rows(0).getAs[Array[Byte]](0).sameElements(testBytes))
    assert(rows(0).getString(1).equals("Sensitive"))
    assert(rows(0).getString(2).equals("Insensitive"))
    assert(rows(0).getString(3).equals("Twenty-byte CHAR"))
    assert(rows(0).getAs[Array[Byte]](4).sameElements(testBytes))
    assert(rows(0).getString(5).equals("I am a clob!"))
  }

  test("H2 time types") {
    val rows = sql("SELECT * FROM timetypes").collect()
    val cal = new GregorianCalendar(java.util.Locale.ROOT)
    cal.setTime(rows(0).getAs[java.sql.Timestamp](0))
    assert(cal.get(Calendar.HOUR_OF_DAY) === 12)
    assert(cal.get(Calendar.MINUTE) === 34)
    assert(cal.get(Calendar.SECOND) === 56)
    cal.setTime(rows(0).getAs[java.sql.Timestamp](1))
    assert(cal.get(Calendar.YEAR) === 1996)
    assert(cal.get(Calendar.MONTH) === 0)
    assert(cal.get(Calendar.DAY_OF_MONTH) === 1)
    cal.setTime(rows(0).getAs[java.sql.Timestamp](2))
    assert(cal.get(Calendar.YEAR) === 2002)
    assert(cal.get(Calendar.MONTH) === 1)
    assert(cal.get(Calendar.DAY_OF_MONTH) === 20)
    assert(cal.get(Calendar.HOUR) === 11)
    assert(cal.get(Calendar.MINUTE) === 22)
    assert(cal.get(Calendar.SECOND) === 33)
    assert(rows(0).getAs[java.sql.Timestamp](2).getNanos === 543543000)
  }

  test("test DATE types") {
    val rows = spark.read.jdbc(
      urlWithUserAndPass, "TEST.TIMETYPES", new Properties()).collect()
    val cachedRows = spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties())
      .cache().collect()
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    assert(rows(1).getAs[java.sql.Date](1) === null)
    assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
  }

  test("test DATE types in cache") {
    withTempView("mycached_date") {
      val rows = spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties()).collect()
      spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties())
        .cache().createOrReplaceTempView("mycached_date")
      val cachedRows = sql("select * from mycached_date").collect()
      assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
      assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    }
  }

  test("test types for null value") {
    val rows = spark.read.jdbc(
      urlWithUserAndPass, "TEST.NULLTYPES", new Properties()).collect()
    assert((0 to 14).forall(i => rows(0).isNullAt(i)))
  }

  test("H2 floating-point types") {
    val rows = sql("SELECT * FROM flttypes").collect()
    assert(rows(0).getDouble(0) === 1.00000000000000022)
    assert(rows(0).getDouble(1) === 1.00000011920928955)
    assert(rows(0).getAs[BigDecimal](2) ===
      new BigDecimal("123456789012345.543215432154321000"))
    assert(rows(0).schema.fields(2).dataType === DecimalType(38, 18))
    val result = sql("SELECT C FROM flttypes where C > C - 1").collect()
    assert(result(0).getAs[BigDecimal](0) ===
      new BigDecimal("123456789012345.543215432154321000"))
  }

  test("SQL query as table name") {
    sql(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW hack
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable '(SELECT B, B*B FROM TEST.FLTTYPES)',
        |         user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))
    val rows = sql("SELECT * FROM hack").collect()
    assert(rows(0).getDouble(0) === 1.00000011920928955) // Yes, I meant ==.
    // For some reason, H2 computes this square incorrectly...
    assert(math.abs(rows(0).getDouble(1) - 1.00000023841859331) < 1e-12)
  }

  test("Pass extra properties via OPTIONS") {
    // We set rowId to false during setup, which means that _ROWID_ column should be absent from
    // all tables. If rowId is true (default), the query below doesn't throw an exception.
    intercept[JdbcSQLException] {
      sql(
        s"""
          |CREATE OR REPLACE TEMPORARY VIEW abc
          |USING org.apache.spark.sql.jdbc
          |OPTIONS (url '$url', dbtable '(SELECT _ROWID_ FROM test.people)',
          |         user 'testUser', password 'testPass')
         """.stripMargin.replaceAll("\n", " "))
    }
  }

  test("Remap types via JdbcDialects") {
    JdbcDialects.registerDialect(testH2Dialect)
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", new Properties())
    assert(!df.schema.exists(_.dataType != org.apache.spark.sql.types.StringType))
    val rows = df.collect()
    assert(rows(0).get(0).isInstanceOf[String])
    assert(rows(0).get(1).isInstanceOf[String])
    JdbcDialects.unregisterDialect(testH2Dialect)
  }

  test("Map TINYINT to ByteType via JdbcDialects") {
    JdbcDialects.registerDialect(testH2DialectTinyInt)
    val df = spark.read.jdbc(urlWithUserAndPass, "test.inttypes", new Properties())
    val rows = df.collect()
    assert(rows.length === 2)
    assert(rows(0).get(2).isInstanceOf[Byte])
    assert(rows(0).getByte(2) === 3)
    assert(rows(1).isNullAt(2))
    JdbcDialects.unregisterDialect(testH2DialectTinyInt)
  }

  test("Default jdbc dialect registration") {
    assert(JdbcDialects.get("jdbc:mysql://127.0.0.1/db") == MySQLDialect)
    assert(JdbcDialects.get("jdbc:postgresql://127.0.0.1/db") == PostgresDialect)
    assert(JdbcDialects.get("jdbc:db2://127.0.0.1/db") == DB2Dialect)
    assert(JdbcDialects.get("jdbc:sqlserver://127.0.0.1/db") == MsSqlServerDialect)
    assert(JdbcDialects.get("jdbc:derby:db") == DerbyDialect)
    assert(JdbcDialects.get("test.invalid") == NoopDialect)
  }

  test("quote column names by jdbc dialect") {
    val MySQL = JdbcDialects.get("jdbc:mysql://127.0.0.1/db")
    val Postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    val Derby = JdbcDialects.get("jdbc:derby:db")

    val columns = Seq("abc", "key")
    val MySQLColumns = columns.map(MySQL.quoteIdentifier(_))
    val PostgresColumns = columns.map(Postgres.quoteIdentifier(_))
    val DerbyColumns = columns.map(Derby.quoteIdentifier(_))
    assert(MySQLColumns === Seq("`abc`", "`key`"))
    assert(PostgresColumns === Seq(""""abc"""", """"key""""))
    assert(DerbyColumns === Seq(""""abc"""", """"key""""))
  }

  test("compile filters") {
    val compileFilter = PrivateMethod[Option[String]](Symbol("compileFilter"))
    def doCompileFilter(f: Filter): String =
      JDBCRDD invokePrivate compileFilter(f, JdbcDialects.get("jdbc:")) getOrElse("")
    assert(doCompileFilter(EqualTo("col0", 3)) === """"col0" = 3""")
    assert(doCompileFilter(Not(EqualTo("col1", "abc"))) === """(NOT ("col1" = 'abc'))""")
    assert(doCompileFilter(And(EqualTo("col0", 0), EqualTo("col1", "def")))
      === """("col0" = 0) AND ("col1" = 'def')""")
    assert(doCompileFilter(Or(EqualTo("col0", 2), EqualTo("col1", "ghi")))
      === """("col0" = 2) OR ("col1" = 'ghi')""")
    assert(doCompileFilter(LessThan("col0", 5)) === """"col0" < 5""")
    assert(doCompileFilter(LessThan("col3",
      Timestamp.valueOf("1995-11-21 00:00:00.0"))) === """"col3" < '1995-11-21 00:00:00.0'""")
    assert(doCompileFilter(LessThan("col4", Date.valueOf("1983-08-04")))
      === """"col4" < '1983-08-04'""")
    assert(doCompileFilter(LessThanOrEqual("col0", 5)) === """"col0" <= 5""")
    assert(doCompileFilter(GreaterThan("col0", 3)) === """"col0" > 3""")
    assert(doCompileFilter(GreaterThanOrEqual("col0", 3)) === """"col0" >= 3""")
    assert(doCompileFilter(In("col1", Array("jkl"))) === """"col1" IN ('jkl')""")
    assert(doCompileFilter(In("col1", Array.empty)) ===
      """CASE WHEN "col1" IS NULL THEN NULL ELSE FALSE END""")
    assert(doCompileFilter(Not(In("col1", Array("mno", "pqr"))))
      === """(NOT ("col1" IN ('mno', 'pqr')))""")
    assert(doCompileFilter(IsNull("col1")) === """"col1" IS NULL""")
    assert(doCompileFilter(IsNotNull("col1")) === """"col1" IS NOT NULL""")
    assert(doCompileFilter(And(EqualNullSafe("col0", "abc"), EqualTo("col1", "def")))
      === """((NOT ("col0" != 'abc' OR "col0" IS NULL OR 'abc' IS NULL) """
        + """OR ("col0" IS NULL AND 'abc' IS NULL))) AND ("col1" = 'def')""")
  }

  test("Dialect unregister") {
    JdbcDialects.unregisterDialect(H2Dialect)
    try {
      JdbcDialects.registerDialect(testH2Dialect)
      JdbcDialects.unregisterDialect(testH2Dialect)
      assert(JdbcDialects.get(urlWithUserAndPass) == NoopDialect)
    } finally {
      JdbcDialects.registerDialect(H2Dialect)
    }
  }

  test("Aggregated dialects") {
    val agg = new AggregatedDialect(List(new JdbcDialect {
      override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2:")
      override def getCatalystType(
          sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
        if (sqlType % 2 == 0) {
          Some(LongType)
        } else {
          None
        }
      override def quoteIdentifier(colName: String): String = {
        s"My $colName quoteIdentifier"
      }
      override def getTableExistsQuery(table: String): String = {
        s"My $table Table"
      }
      override def getSchemaQuery(table: String): String = {
        s"My $table Schema"
      }
      override def isCascadingTruncateTable(): Option[Boolean] = Some(true)
    }, testH2Dialect))
    assert(agg.canHandle("jdbc:h2:xxx"))
    assert(!agg.canHandle("jdbc:h2"))
    assert(agg.getCatalystType(0, "", 1, null) === Some(LongType))
    assert(agg.getCatalystType(1, "", 1, null) === Some(StringType))
    assert(agg.isCascadingTruncateTable() === Some(true))
    assert(agg.quoteIdentifier ("Dummy") === "My Dummy quoteIdentifier")
    assert(agg.getTableExistsQuery ("Dummy") === "My Dummy Table")
    assert(agg.getSchemaQuery ("Dummy") === "My Dummy Schema")
  }

  test("Aggregated dialects: isCascadingTruncateTable") {
    def genDialect(cascadingTruncateTable: Option[Boolean]): JdbcDialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = true
      override def getCatalystType(
        sqlType: Int,
        typeName: String,
        size: Int,
        md: MetadataBuilder): Option[DataType] = None
      override def isCascadingTruncateTable(): Option[Boolean] = cascadingTruncateTable
    }

    def testDialects(cascadings: List[Option[Boolean]], expected: Option[Boolean]): Unit = {
      val dialects = cascadings.map(genDialect(_))
      val agg = new AggregatedDialect(dialects)
      assert(agg.isCascadingTruncateTable() === expected)
    }

    testDialects(List(Some(true), Some(false), None), Some(true))
    testDialects(List(Some(true), Some(true), None), Some(true))
    testDialects(List(Some(false), Some(false), None), None)
    testDialects(List(Some(true), Some(true)), Some(true))
    testDialects(List(Some(false), Some(false)), Some(false))
    testDialects(List(None, None), None)
  }

  test("DB2Dialect type mapping") {
    val db2Dialect = JdbcDialects.get("jdbc:db2://127.0.0.1/db")
    assert(db2Dialect.getJDBCType(StringType).map(_.databaseTypeDefinition).get == "CLOB")
    assert(db2Dialect.getJDBCType(BooleanType).map(_.databaseTypeDefinition).get == "CHAR(1)")
    assert(db2Dialect.getJDBCType(ShortType).map(_.databaseTypeDefinition).get == "SMALLINT")
    assert(db2Dialect.getJDBCType(ByteType).map(_.databaseTypeDefinition).get == "SMALLINT")
    // test db2 dialect mappings on read
    assert(db2Dialect.getCatalystType(java.sql.Types.REAL, "REAL", 1, null) == Option(FloatType))
    assert(db2Dialect.getCatalystType(java.sql.Types.OTHER, "DECFLOAT", 1, null) ==
      Option(DecimalType(38, 18)))
    assert(db2Dialect.getCatalystType(java.sql.Types.OTHER, "XML", 1, null) == Option(StringType))
    assert(db2Dialect.getCatalystType(java.sql.Types.OTHER, "TIMESTAMP WITH TIME ZONE", 1, null) ==
      Option(TimestampType))
  }

  test("PostgresDialect type mapping") {
    val Postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    val md = new MetadataBuilder().putLong("scale", 0)
    assert(Postgres.getCatalystType(java.sql.Types.OTHER, "json", 1, null) === Some(StringType))
    assert(Postgres.getCatalystType(java.sql.Types.OTHER, "jsonb", 1, null) === Some(StringType))
    assert(Postgres.getCatalystType(java.sql.Types.ARRAY, "_numeric", 0, md) ==
      Some(ArrayType(DecimalType.SYSTEM_DEFAULT)))
    assert(Postgres.getCatalystType(java.sql.Types.ARRAY, "_bpchar", 64, md) ==
      Some(ArrayType(StringType)))
    assert(Postgres.getJDBCType(FloatType).map(_.databaseTypeDefinition).get == "FLOAT4")
    assert(Postgres.getJDBCType(DoubleType).map(_.databaseTypeDefinition).get == "FLOAT8")
    assert(Postgres.getJDBCType(ByteType).map(_.databaseTypeDefinition).get == "SMALLINT")
  }

  test("DerbyDialect jdbc type mapping") {
    val derbyDialect = JdbcDialects.get("jdbc:derby:db")
    assert(derbyDialect.getJDBCType(StringType).map(_.databaseTypeDefinition).get == "CLOB")
    assert(derbyDialect.getJDBCType(ByteType).map(_.databaseTypeDefinition).get == "SMALLINT")
    assert(derbyDialect.getJDBCType(BooleanType).map(_.databaseTypeDefinition).get == "BOOLEAN")
  }

  test("OracleDialect jdbc type mapping") {
    val oracleDialect = JdbcDialects.get("jdbc:oracle")
    val metadata = new MetadataBuilder().putString("name", "test_column").putLong("scale", -127)
    assert(oracleDialect.getCatalystType(java.sql.Types.NUMERIC, "float", 1, metadata) ==
      Some(DecimalType(DecimalType.MAX_PRECISION, 10)))
    assert(oracleDialect.getCatalystType(java.sql.Types.NUMERIC, "numeric", 0, null) ==
      Some(DecimalType(DecimalType.MAX_PRECISION, 10)))
    assert(oracleDialect.getCatalystType(OracleDialect.BINARY_FLOAT, "BINARY_FLOAT", 0, null) ==
      Some(FloatType))
    assert(oracleDialect.getCatalystType(OracleDialect.BINARY_DOUBLE, "BINARY_DOUBLE", 0, null) ==
      Some(DoubleType))
    assert(oracleDialect.getCatalystType(OracleDialect.TIMESTAMPTZ, "TIMESTAMP", 0, null) ==
      Some(TimestampType))
  }

  test("MsSqlServerDialect jdbc type mapping") {
    val msSqlServerDialect = JdbcDialects.get("jdbc:sqlserver")
    assert(msSqlServerDialect.getJDBCType(TimestampType).map(_.databaseTypeDefinition).get ==
      "DATETIME")
    assert(msSqlServerDialect.getJDBCType(StringType).map(_.databaseTypeDefinition).get ==
      "NVARCHAR(MAX)")
    assert(msSqlServerDialect.getJDBCType(BooleanType).map(_.databaseTypeDefinition).get ==
      "BIT")
    assert(msSqlServerDialect.getJDBCType(BinaryType).map(_.databaseTypeDefinition).get ==
      "VARBINARY(MAX)")
    Seq(true, false).foreach { flag =>
      withSQLConf(SQLConf.LEGACY_MSSQLSERVER_NUMERIC_MAPPING_ENABLED.key -> s"$flag") {
        if (SQLConf.get.legacyMsSqlServerNumericMappingEnabled) {
          assert(msSqlServerDialect.getJDBCType(ShortType).map(_.databaseTypeDefinition).isEmpty)
        } else {
          assert(msSqlServerDialect.getJDBCType(ShortType).map(_.databaseTypeDefinition).get ==
            "SMALLINT")
        }
      }
    }
  }

  test("SPARK-28152 MsSqlServerDialect catalyst type mapping") {
    val msSqlServerDialect = JdbcDialects.get("jdbc:sqlserver")
    val metadata = new MetadataBuilder().putLong("scale", 1)

    Seq(true, false).foreach { flag =>
      withSQLConf(SQLConf.LEGACY_MSSQLSERVER_NUMERIC_MAPPING_ENABLED.key -> s"$flag") {
        if (SQLConf.get.legacyMsSqlServerNumericMappingEnabled) {
          assert(msSqlServerDialect.getCatalystType(java.sql.Types.SMALLINT, "SMALLINT", 1,
            metadata).isEmpty)
          assert(msSqlServerDialect.getCatalystType(java.sql.Types.REAL, "REAL", 1,
            metadata).isEmpty)
        } else {
          assert(msSqlServerDialect.getCatalystType(java.sql.Types.SMALLINT, "SMALLINT", 1,
            metadata).get == ShortType)
          assert(msSqlServerDialect.getCatalystType(java.sql.Types.REAL, "REAL", 1,
            metadata).get == FloatType)
        }
      }
    }
  }

  test("table exists query by jdbc dialect") {
    val MySQL = JdbcDialects.get("jdbc:mysql://127.0.0.1/db")
    val Postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    val db2 = JdbcDialects.get("jdbc:db2://127.0.0.1/db")
    val h2 = JdbcDialects.get(url)
    val derby = JdbcDialects.get("jdbc:derby:db")
    val table = "weblogs"
    val defaultQuery = s"SELECT * FROM $table WHERE 1=0"
    val limitQuery = s"SELECT 1 FROM $table LIMIT 1"
    assert(MySQL.getTableExistsQuery(table) == limitQuery)
    assert(Postgres.getTableExistsQuery(table) == limitQuery)
    assert(db2.getTableExistsQuery(table) == defaultQuery)
    assert(h2.getTableExistsQuery(table) == defaultQuery)
    assert(derby.getTableExistsQuery(table) == defaultQuery)
  }

  test("truncate table query by jdbc dialect") {
    val mysql = JdbcDialects.get("jdbc:mysql://127.0.0.1/db")
    val postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    val db2 = JdbcDialects.get("jdbc:db2://127.0.0.1/db")
    val h2 = JdbcDialects.get(url)
    val derby = JdbcDialects.get("jdbc:derby:db")
    val oracle = JdbcDialects.get("jdbc:oracle://127.0.0.1/db")
    val teradata = JdbcDialects.get("jdbc:teradata://127.0.0.1/db")

    val table = "weblogs"
    val defaultQuery = s"TRUNCATE TABLE $table"
    val postgresQuery = s"TRUNCATE TABLE ONLY $table"
    val teradataQuery = s"DELETE FROM $table ALL"

    Seq(mysql, db2, h2, derby).foreach{ dialect =>
      assert(dialect.getTruncateQuery(table, Some(true)) == defaultQuery)
    }

    assert(postgres.getTruncateQuery(table) == postgresQuery)
    assert(oracle.getTruncateQuery(table) == defaultQuery)
    assert(teradata.getTruncateQuery(table) == teradataQuery)
  }

  test("SPARK-22880: Truncate table with CASCADE by jdbc dialect") {
    // cascade in a truncate should only be applied for databases that support this,
    // even if the parameter is passed.
    val mysql = JdbcDialects.get("jdbc:mysql://127.0.0.1/db")
    val postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    val db2 = JdbcDialects.get("jdbc:db2://127.0.0.1/db")
    val h2 = JdbcDialects.get(url)
    val derby = JdbcDialects.get("jdbc:derby:db")
    val oracle = JdbcDialects.get("jdbc:oracle://127.0.0.1/db")
    val teradata = JdbcDialects.get("jdbc:teradata://127.0.0.1/db")

    val table = "weblogs"
    val defaultQuery = s"TRUNCATE TABLE $table"
    val postgresQuery = s"TRUNCATE TABLE ONLY $table CASCADE"
    val oracleQuery = s"TRUNCATE TABLE $table CASCADE"
    val teradataQuery = s"DELETE FROM $table ALL"

    Seq(mysql, db2, h2, derby).foreach{ dialect =>
      assert(dialect.getTruncateQuery(table, Some(true)) == defaultQuery)
    }
    assert(postgres.getTruncateQuery(table, Some(true)) == postgresQuery)
    assert(oracle.getTruncateQuery(table, Some(true)) == oracleQuery)
    assert(teradata.getTruncateQuery(table, Some(true)) == teradataQuery)
  }

  test("Test DataFrame.where for Date and Timestamp") {
    // Regression test for bug SPARK-11788
    val timestamp = java.sql.Timestamp.valueOf("2001-02-20 11:22:33.543543");
    val date = java.sql.Date.valueOf("1995-01-01")
    val jdbcDf = spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties())
    val rows = jdbcDf.where($"B" > date && $"C" > timestamp).collect()
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    assert(rows(0).getAs[java.sql.Timestamp](2)
      === java.sql.Timestamp.valueOf("2002-02-20 11:22:33.543543"))
  }

  test("SPARK-33867: Test DataFrame.where for LocalDate and Instant") {
    // Test for SPARK-33867
    val timestamp = Instant.parse("2001-02-20T11:22:33.543543Z")
    val date = LocalDate.parse("1995-01-01")
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val jdbcDf = spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties())
      val rows = jdbcDf.where($"B" > date && $"C" > timestamp).collect()
      assert(rows(0).getAs[LocalDate](1) === LocalDate.parse("1996-01-01"))
      // 8 hour difference since saved time was America/Los_Angeles and Instant is GMT
      assert(rows(0).getAs[Instant](2) === Instant.parse("2002-02-20T19:22:33.543543Z"))
    }
  }

  test("test credentials in the properties are not in plan output") {
    val df = sql("SELECT * FROM parts")
    val explain = ExplainCommand(df.queryExecution.logical, ExtendedMode)
    spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => assert(!List("testPass", "testUser").exists(r.toString.contains))
    }
    // test the JdbcRelation toString output
    df.queryExecution.analyzed.collect {
      case r: LogicalRelation =>
        assert(r.relation.toString == "JDBCRelation(TEST.PEOPLE) [numPartitions=3]")
    }
  }

  test("test credentials in the connection url are not in the plan output") {
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", new Properties())
    val explain = ExplainCommand(df.queryExecution.logical, ExtendedMode)
    spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => assert(!List("testPass", "testUser").exists(r.toString.contains))
    }
  }

  test("hide credentials in create and describe a persistent/temp table") {
    val password = "testPass"
    val tableName = "tab1"
    Seq("TABLE", "TEMPORARY VIEW").foreach { tableType =>
      withTable(tableName) {
        val df = sql(
          s"""
             |CREATE $tableType $tableName
             |USING org.apache.spark.sql.jdbc
             |OPTIONS (
             | url '$urlWithUserAndPass',
             | dbtable 'TEST.PEOPLE',
             | user 'testUser',
             | password '$password')
           """.stripMargin)

        val explain = ExplainCommand(df.queryExecution.logical, ExtendedMode)
        spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach { r =>
          assert(!r.toString.contains(password))
        }

        sql(s"DESC FORMATTED $tableName").collect().foreach { r =>
          assert(!r.toString().contains(password))
        }
      }
    }
  }

  test("Hide credentials in show create table") {
    val userName = "testUser"
    val password = "testPass"
    val tableName = "tab1"
    val dbTable = "TEST.PEOPLE"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName
           |USING org.apache.spark.sql.jdbc
           |OPTIONS (
           | url '$urlWithUserAndPass',
           | dbtable '$dbTable',
           | user '$userName',
           | password '$password')
         """.stripMargin)

      val show = ShowCreateTableCommand(TableIdentifier(tableName))
      spark.sessionState.executePlan(show).executedPlan.executeCollect().foreach { r =>
        assert(!r.toString.contains(password))
        assert(r.toString.contains(dbTable))
        assert(r.toString.contains(userName))
      }

      sql(s"SHOW CREATE TABLE $tableName").collect().foreach { r =>
        assert(!r.toString.contains(password))
        assert(r.toString.contains(dbTable))
        assert(r.toString.contains(userName))
      }

      withSQLConf(SQLConf.SQL_OPTIONS_REDACTION_PATTERN.key -> "(?i)dbtable|user") {
        spark.sessionState.executePlan(show).executedPlan.executeCollect().foreach { r =>
          assert(!r.toString.contains(password))
          assert(!r.toString.contains(dbTable))
          assert(!r.toString.contains(userName))
        }
      }
    }
  }

  test("Replace CatalogUtils.maskCredentials with SQLConf.get.redactOptions") {
    val password = "testPass"
    val tableName = "tab1"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName
           |USING org.apache.spark.sql.jdbc
           |OPTIONS (
           | url '$urlWithUserAndPass',
           | dbtable 'TEST.PEOPLE',
           | user 'testUser',
           | password '$password')
         """.stripMargin)

      val storageProps = sql(s"DESC FORMATTED $tableName")
        .filter("col_name = 'Storage Properties'")
        .select("data_type").collect()
      assert(storageProps.length === 1)
      storageProps.foreach { r =>
        assert(r.getString(0).contains(s"url=${Utils.REDACTION_REPLACEMENT_TEXT}"))
        assert(r.getString(0).contains(s"password=${Utils.REDACTION_REPLACEMENT_TEXT}"))
      }

      val information = sql(s"SHOW TABLE EXTENDED LIKE '$tableName'")
        .select("information").collect()
      assert(information.length === 1)
      information.foreach { r =>
        assert(r.getString(0).contains(s"url=${Utils.REDACTION_REPLACEMENT_TEXT}"))
        assert(r.getString(0).contains(s"password=${Utils.REDACTION_REPLACEMENT_TEXT}"))
      }

      val createTabStmt = sql(s"SHOW CREATE TABLE $tableName")
        .select("createtab_stmt").collect()
      assert(createTabStmt.length === 1)
      createTabStmt.foreach { r =>
        assert(r.getString(0).contains(s"`url` '${Utils.REDACTION_REPLACEMENT_TEXT}'"))
        assert(r.getString(0).contains(s"`password` '${Utils.REDACTION_REPLACEMENT_TEXT}'"))
      }
    }
  }

  test("SPARK 12941: The data type mapping for StringType to Oracle") {
    val oracleDialect = JdbcDialects.get("jdbc:oracle://127.0.0.1/db")
    assert(oracleDialect.getJDBCType(StringType).
      map(_.databaseTypeDefinition).get == "VARCHAR2(255)")
  }

  test("SPARK-16625: General data types to be mapped to Oracle") {

    def getJdbcType(dialect: JdbcDialect, dt: DataType): String = {
      dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).
        map(_.databaseTypeDefinition).get
    }

    val oracleDialect = JdbcDialects.get("jdbc:oracle://127.0.0.1/db")
    assert(getJdbcType(oracleDialect, BooleanType) == "NUMBER(1)")
    assert(getJdbcType(oracleDialect, IntegerType) == "NUMBER(10)")
    assert(getJdbcType(oracleDialect, LongType) == "NUMBER(19)")
    assert(getJdbcType(oracleDialect, FloatType) == "NUMBER(19, 4)")
    assert(getJdbcType(oracleDialect, DoubleType) == "NUMBER(19, 4)")
    assert(getJdbcType(oracleDialect, ByteType) == "NUMBER(3)")
    assert(getJdbcType(oracleDialect, ShortType) == "NUMBER(5)")
    assert(getJdbcType(oracleDialect, StringType) == "VARCHAR2(255)")
    assert(getJdbcType(oracleDialect, BinaryType) == "BLOB")
    assert(getJdbcType(oracleDialect, DateType) == "DATE")
    assert(getJdbcType(oracleDialect, TimestampType) == "TIMESTAMP")
  }

  private def assertEmptyQuery(sqlString: String): Unit = {
    assert(sql(sqlString).collect().isEmpty)
  }

  test("SPARK-15916: JDBC filter operator push down should respect operator precedence") {
    val TRUE = "NAME != 'non_exists'"
    val FALSE1 = "THEID > 1000000000"
    val FALSE2 = "THEID < -1000000000"

    assertEmptyQuery(s"SELECT * FROM foobar WHERE ($TRUE OR $FALSE1) AND $FALSE2")
    assertEmptyQuery(s"SELECT * FROM foobar WHERE $FALSE1 AND ($FALSE2 OR $TRUE)")

    // Tests JDBCPartition whereClause clause push down.
    withTempView("tempFrame") {
      val jdbcPartitionWhereClause = s"$FALSE1 OR $TRUE"
      val df = spark.read.jdbc(
        urlWithUserAndPass,
        "TEST.PEOPLE",
        predicates = Array[String](jdbcPartitionWhereClause),
        new Properties())

      df.createOrReplaceTempView("tempFrame")
      assertEmptyQuery(s"SELECT * FROM tempFrame where $FALSE2")
    }
  }

  test("SPARK-16387: Reserved SQL words are not escaped by JDBC writer") {
    val df = spark.createDataset(Seq("a", "b", "c")).toDF("order")
    val schema = JdbcUtils.schemaString(
      df.schema,
      df.sqlContext.conf.caseSensitiveAnalysis,
      "jdbc:mysql://localhost:3306/temp")
    assert(schema.contains("`order` TEXT"))
  }

  test("SPARK-18141: Predicates on quoted column names in the jdbc data source") {
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id < 1").collect().size == 0)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id <= 1").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id > 1").collect().size == 2)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id >= 1").collect().size == 3)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id = 1").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id != 2").collect().size == 2)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id <=> 2").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name LIKE 'fr%'").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name LIKE '%ed'").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name LIKE '%re%'").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name IS NULL").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name IS NOT NULL").collect().size == 2)
    assert(sql("SELECT * FROM mixedCaseCols").filter($"Name".isin()).collect().size == 0)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name IN ('mary', 'fred')").collect().size == 2)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name NOT IN ('fred')").collect().size == 1)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Id = 1 OR Name = 'mary'").collect().size == 2)
    assert(sql("SELECT * FROM mixedCaseCols WHERE Name = 'mary' AND Id = 2").collect().size == 1)
  }

  test("SPARK-18419: Fix `asConnectionProperties` to filter case-insensitively") {
    val parameters = Map(
      "url" -> "jdbc:mysql://localhost:3306/temp",
      "dbtable" -> "t1",
      "numPartitions" -> "10")
    assert(new JDBCOptions(parameters).asConnectionProperties.isEmpty)
    assert(new JDBCOptions(CaseInsensitiveMap(parameters)).asConnectionProperties.isEmpty)
  }

  test("SPARK-16848: jdbc API throws an exception for user specified schema") {
    val schema = StructType(Seq(
      StructField("name", StringType, false), StructField("theid", IntegerType, false)))
    val parts = Array[String]("THEID < 2", "THEID >= 2")
    val e1 = intercept[AnalysisException] {
      spark.read.schema(schema).jdbc(urlWithUserAndPass, "TEST.PEOPLE", parts, new Properties())
    }.getMessage
    assert(e1.contains("User specified schema not supported with `jdbc`"))

    val e2 = intercept[AnalysisException] {
      spark.read.schema(schema).jdbc(urlWithUserAndPass, "TEST.PEOPLE", new Properties())
    }.getMessage
    assert(e2.contains("User specified schema not supported with `jdbc`"))
  }

  test("jdbc API support custom schema") {
    val parts = Array[String]("THEID < 2", "THEID >= 2")
    val customSchema = "NAME STRING, THEID INT"
    val props = new Properties()
    props.put("customSchema", customSchema)
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", parts, props)
    assert(df.schema.size === 2)
    assert(df.schema === CatalystSqlParser.parseTableSchema(customSchema))
    assert(df.count() === 3)
  }

  test("jdbc API custom schema DDL-like strings.") {
    withTempView("people_view") {
      val customSchema = "NAME STRING, THEID INT"
      sql(
        s"""
           |CREATE TEMPORARY VIEW people_view
           |USING org.apache.spark.sql.jdbc
           |OPTIONS (uRl '$url', DbTaBlE 'TEST.PEOPLE', User 'testUser', PassWord 'testPass',
           |customSchema '$customSchema')
        """.stripMargin.replaceAll("\n", " "))
      val df = sql("select * from people_view")
      assert(df.schema.length === 2)
      assert(df.schema === CatalystSqlParser.parseTableSchema(customSchema))
      assert(df.count() === 3)
    }
  }

  test("SPARK-15648: teradataDialect StringType data mapping") {
    val teradataDialect = JdbcDialects.get("jdbc:teradata://127.0.0.1/db")
    assert(teradataDialect.getJDBCType(StringType).
      map(_.databaseTypeDefinition).get == "VARCHAR(255)")
  }

  test("SPARK-15648: teradataDialect BooleanType data mapping") {
    val teradataDialect = JdbcDialects.get("jdbc:teradata://127.0.0.1/db")
    assert(teradataDialect.getJDBCType(BooleanType).
      map(_.databaseTypeDefinition).get == "CHAR(1)")
  }

  test("Checking metrics correctness with JDBC") {
    val foobarCnt = spark.table("foobar").count()
    val res = InputOutputMetricsHelper.run(sql("SELECT * FROM foobar").toDF())
    assert(res === (foobarCnt, 0L, foobarCnt) :: Nil)
  }

  test("unsupported types") {
    var e = intercept[SQLException] {
      spark.read.jdbc(urlWithUserAndPass, "TEST.TIMEZONE", new Properties()).collect()
    }.getMessage
    assert(e.contains("Unsupported type TIMESTAMP_WITH_TIMEZONE"))
    e = intercept[SQLException] {
      spark.read.jdbc(urlWithUserAndPass, "TEST.ARRAY", new Properties()).collect()
    }.getMessage
    assert(e.contains("Unsupported type ARRAY"))
  }

  test("SPARK-19318: Connection properties keys should be case-sensitive.") {
    def testJdbcOptions(options: JDBCOptions): Unit = {
      // Spark JDBC data source options are case-insensitive
      assert(options.tableOrQuery == "t1")
      // When we convert it to properties, it should be case-sensitive.
      assert(options.asProperties.size == 3)
      assert(options.asProperties.get("customkey") == null)
      assert(options.asProperties.get("customKey") == "a-value")
      assert(options.asConnectionProperties.size == 1)
      assert(options.asConnectionProperties.get("customkey") == null)
      assert(options.asConnectionProperties.get("customKey") == "a-value")
    }

    val parameters = Map("url" -> url, "dbTAblE" -> "t1", "customKey" -> "a-value")
    testJdbcOptions(new JDBCOptions(parameters))
    testJdbcOptions(new JDBCOptions(CaseInsensitiveMap(parameters)))
    // test add/remove key-value from the case-insensitive map
    var modifiedParameters =
      (CaseInsensitiveMap(Map.empty) ++ parameters).asInstanceOf[Map[String, String]]
    testJdbcOptions(new JDBCOptions(modifiedParameters))
    modifiedParameters -= "dbtable"
    assert(modifiedParameters.get("dbTAblE").isEmpty)
    modifiedParameters -= "customkey"
    assert(modifiedParameters.get("customKey").isEmpty)
    modifiedParameters += ("customKey" -> "a-value")
    modifiedParameters += ("dbTable" -> "t1")
    testJdbcOptions(new JDBCOptions(modifiedParameters))
    assert ((modifiedParameters -- parameters.keys).size == 0)
  }

  test("SPARK-19318: jdbc data source options should be treated case-insensitive.") {
    val df = spark.read.format("jdbc")
      .option("Url", urlWithUserAndPass)
      .option("DbTaBle", "TEST.PEOPLE")
      .load()
    assert(df.count() == 3)

    withTempView("people_view") {
      sql(
        s"""
          |CREATE TEMPORARY VIEW people_view
          |USING org.apache.spark.sql.jdbc
          |OPTIONS (uRl '$url', DbTaBlE 'TEST.PEOPLE', User 'testUser', PassWord 'testPass')
        """.stripMargin.replaceAll("\n", " "))

      assert(sql("select * from people_view").count() == 3)
    }
  }

  test("SPARK-21519: option sessionInitStatement, run SQL to initialize the database session.") {
    val initSQL1 = "SET @MYTESTVAR 21519"
    val df1 = spark.read.format("jdbc")
      .option("url", urlWithUserAndPass)
      .option("dbtable", "(SELECT NVL(@MYTESTVAR, -1))")
      .option("sessionInitStatement", initSQL1)
      .load()
    assert(df1.collect() === Array(Row(21519)))

    val initSQL2 = "SET SCHEMA DUMMY"
    val df2 = spark.read.format("jdbc")
      .option("url", urlWithUserAndPass)
      .option("dbtable", "TEST.PEOPLE")
      .option("sessionInitStatement", initSQL2)
      .load()
    val e = intercept[SparkException] {df2.collect()}.getMessage
    assert(e.contains("""Schema "DUMMY" not found"""))

    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW test_sessionInitStatement
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$urlWithUserAndPass',
         |dbtable '(SELECT NVL(@MYTESTVAR1, -1), NVL(@MYTESTVAR2, -1))',
         |sessionInitStatement 'SET @MYTESTVAR1 21519; SET @MYTESTVAR2 1234')
       """.stripMargin)

      val df3 = sql("SELECT * FROM test_sessionInitStatement")
      assert(df3.collect() === Array(Row(21519, 1234)))
    }

  test("jdbc data source shouldn't have unnecessary metadata in its schema") {
    val schema = StructType(Seq(
      StructField("NAME", StringType, true), StructField("THEID", IntegerType, true)))

    val df = spark.read.format("jdbc")
      .option("Url", urlWithUserAndPass)
      .option("DbTaBle", "TEST.PEOPLE")
      .load()
    assert(df.schema === schema)

    withTempView("people_view") {
      sql(
        s"""
          |CREATE TEMPORARY VIEW people_view
          |USING org.apache.spark.sql.jdbc
          |OPTIONS (uRl '$url', DbTaBlE 'TEST.PEOPLE', User 'testUser', PassWord 'testPass')
        """.stripMargin.replaceAll("\n", " "))

      assert(sql("select * from people_view").schema === schema)
    }
  }

  test("SPARK-23856 Spark jdbc setQueryTimeout option") {
    val numJoins = 100
    val longRunningQuery =
      s"SELECT t0.NAME AS c0, ${(1 to numJoins).map(i => s"t$i.NAME AS c$i").mkString(", ")} " +
        s"FROM test.people t0 ${(1 to numJoins).map(i => s"join test.people t$i").mkString(" ")}"
    val df = spark.read.format("jdbc")
      .option("Url", urlWithUserAndPass)
      .option("dbtable", s"($longRunningQuery)")
      .option("queryTimeout", 1)
      .load()
    val errMsg = intercept[SparkException] {
      df.collect()
    }.getMessage
    assert(errMsg.contains("Statement was canceled or the session timed out"))
  }

  test("SPARK-24327 verify and normalize a partition column based on a JDBC resolved schema") {
    def testJdbcPartitionColumn(partColName: String, expectedColumnName: String): Unit = {
      val df = spark.read.format("jdbc")
        .option("url", urlWithUserAndPass)
        .option("dbtable", "TEST.PARTITION")
        .option("partitionColumn", partColName)
        .option("lowerBound", 1)
        .option("upperBound", 4)
        .option("numPartitions", 3)
        .load()

      val quotedPrtColName = testH2Dialect.quoteIdentifier(expectedColumnName)
      df.logicalPlan match {
        case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
          val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
          assert(whereClauses === Set(
            s"$quotedPrtColName < 2 or $quotedPrtColName is null",
            s"$quotedPrtColName >= 2 AND $quotedPrtColName < 3",
            s"$quotedPrtColName >= 3"))
      }
    }

    testJdbcPartitionColumn("THEID", "THEID")
    testJdbcPartitionColumn("\"THEID\"", "THEID")
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      testJdbcPartitionColumn("ThEiD", "THEID")
    }
    testJdbcPartitionColumn("THE ID", "THE ID")

    def testIncorrectJdbcPartitionColumn(partColName: String): Unit = {
      val errMsg = intercept[AnalysisException] {
        testJdbcPartitionColumn(partColName, "THEID")
      }.getMessage
      assert(errMsg.contains(s"User-defined partition column $partColName not found " +
        "in the JDBC relation:"))
    }

    testIncorrectJdbcPartitionColumn("NoExistingColumn")
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      testIncorrectJdbcPartitionColumn(testH2Dialect.quoteIdentifier("ThEiD"))
    }
  }

  test("query JDBC option - negative tests") {
    val query = "SELECT * FROM  test.people WHERE theid = 1"
    // load path
    val e1 = intercept[RuntimeException] {
      val df = spark.read.format("jdbc")
        .option("Url", urlWithUserAndPass)
        .option("query", query)
        .option("dbtable", "test.people")
        .load()
    }.getMessage
    assert(e1.contains("Both 'dbtable' and 'query' can not be specified at the same time."))

    // jdbc api path
    val properties = new Properties()
    properties.setProperty(JDBCOptions.JDBC_QUERY_STRING, query)
    val e2 = intercept[RuntimeException] {
      spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", properties).collect()
    }.getMessage
    assert(e2.contains("Both 'dbtable' and 'query' can not be specified at the same time."))

    val e3 = intercept[RuntimeException] {
      sql(
        s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', query '$query', dbtable 'TEST.PEOPLE',
         |         user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))
      }.getMessage
    assert(e3.contains("Both 'dbtable' and 'query' can not be specified at the same time."))

    val e4 = intercept[RuntimeException] {
      val df = spark.read.format("jdbc")
        .option("Url", urlWithUserAndPass)
        .option("query", "")
        .load()
    }.getMessage
    assert(e4.contains("Option `query` can not be empty."))

    // Option query and partitioncolumn are not allowed together.
    val expectedErrorMsg =
      s"""
         |Options 'query' and 'partitionColumn' can not be specified together.
         |Please define the query using `dbtable` option instead and make sure to qualify
         |the partition columns using the supplied subquery alias to resolve any ambiguity.
         |Example :
         |spark.read.format("jdbc")
         |  .option("url", jdbcUrl)
         |  .option("dbtable", "(select c1, c2 from t1) as subq")
         |  .option("partitionColumn", "c1")
         |  .option("lowerBound", "1")
         |  .option("upperBound", "100")
         |  .option("numPartitions", "3")
         |  .load()
     """.stripMargin
    val e5 = intercept[RuntimeException] {
      sql(
        s"""
           |CREATE OR REPLACE TEMPORARY VIEW queryOption
           |USING org.apache.spark.sql.jdbc
           |OPTIONS (url '$url', query '$query', user 'testUser', password 'testPass',
           |         partitionColumn 'THEID', lowerBound '1', upperBound '4', numPartitions '3')
       """.stripMargin.replaceAll("\n", " "))
    }.getMessage
    assert(e5.contains(expectedErrorMsg))
  }

  test("query JDBC option") {
    val query = "SELECT name, theid FROM  test.people WHERE theid = 1"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("Url", urlWithUserAndPass)
      .option("query", query)
      .load()
    checkAnswer(
      df,
      Row("fred", 1) :: Nil)

    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', query '$query', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    checkAnswer(
      sql("select name, theid from queryOption"),
      Row("fred", 1) :: Nil)
  }

  test("SPARK-22814 support date/timestamp types in partitionColumn") {
    val expectedResult = Seq(
      ("2018-07-06", "2018-07-06 05:50:00.0"),
      ("2018-07-06", "2018-07-06 08:10:08.0"),
      ("2018-07-08", "2018-07-08 13:32:01.0"),
      ("2018-07-12", "2018-07-12 09:51:15.0")
    ).map { case (date, timestamp) =>
      Row(Date.valueOf(date), Timestamp.valueOf(timestamp))
    }

    // DateType partition column
    val df1 = spark.read.format("jdbc")
      .option("url", urlWithUserAndPass)
      .option("dbtable", "TEST.DATETIME")
      .option("partitionColumn", "d")
      .option("lowerBound", "2018-07-06")
      .option("upperBound", "2018-07-20")
      .option("numPartitions", 3)
      .load()

    df1.logicalPlan match {
      case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
        val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
        assert(whereClauses === Set(
          """"D" < '2018-07-10' or "D" is null""",
          """"D" >= '2018-07-10' AND "D" < '2018-07-14'""",
          """"D" >= '2018-07-14'"""))
    }
    checkAnswer(df1, expectedResult)

    // TimestampType partition column
    val df2 = spark.read.format("jdbc")
      .option("url", urlWithUserAndPass)
      .option("dbtable", "TEST.DATETIME")
      .option("partitionColumn", "t")
      .option("lowerBound", "2018-07-04 03:30:00.0")
      .option("upperBound", "2018-07-27 14:11:05.0")
      .option("numPartitions", 2)
      .load()

    df2.logicalPlan match {
      case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
        val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
        assert(whereClauses === Set(
          """"T" < '2018-07-15 20:50:32.5' or "T" is null""",
          """"T" >= '2018-07-15 20:50:32.5'"""))
    }
    checkAnswer(df2, expectedResult)
  }

  test("throws an exception for unsupported partition column types") {
    val errMsg = intercept[AnalysisException] {
      spark.read.format("jdbc")
        .option("url", urlWithUserAndPass)
        .option("dbtable", "TEST.PEOPLE")
        .option("partitionColumn", "name")
        .option("lowerBound", "aaa")
        .option("upperBound", "zzz")
        .option("numPartitions", 2)
        .load()
    }.getMessage
    assert(errMsg.contains(
      "Partition column type should be numeric, date, or timestamp, but string found."))
  }

  test("SPARK-24288: Enable preventing predicate pushdown") {
    val table = "test.people"

    val df = spark.read.format("jdbc")
      .option("Url", urlWithUserAndPass)
      .option("dbTable", table)
      .option("pushDownPredicate", false)
      .load()
      .filter("theid = 1")
      .select("name", "theid")
    checkAnswer(
      checkNotPushdown(df),
      Row("fred", 1) :: Nil)

    // pushDownPredicate option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW predicateOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$urlWithUserAndPass', dbTable '$table', pushDownPredicate 'false')
       """.stripMargin.replaceAll("\n", " "))
    checkAnswer(
      checkNotPushdown(sql("SELECT name, theid FROM predicateOption WHERE theid = 1")),
      Row("fred", 1) :: Nil)
  }

  test("SPARK-26383 throw IllegalArgumentException if wrong kind of driver to the given url") {
    val e = intercept[IllegalArgumentException] {
      val opts = Map(
        "url" -> "jdbc:mysql://localhost/db",
        "dbtable" -> "table",
        "driver" -> "org.postgresql.Driver"
      )
      spark.read.format("jdbc").options(opts).load
    }.getMessage
    assert(e.contains("The driver could not open a JDBC connection. " +
      "Check the URL: jdbc:mysql://localhost/db"))
  }

  test("support casting patterns for lower/upper bounds of TimestampType") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        Seq(
          ("1972-07-04 03:30:00", "1972-07-15 20:50:32.5", "1972-07-27 14:11:05"),
          ("2019-01-20 12:00:00.502", "2019-01-20 12:00:00.751", "2019-01-20 12:00:01.000"),
          ("2019-01-20T00:00:00.123456", "2019-01-20 00:05:00.123456",
            "2019-01-20T00:10:00.123456"),
          ("1500-01-20T00:00:00.123456", "1500-01-20 00:05:00.123456", "1500-01-20T00:10:00.123456")
        ).foreach { case (lower, middle, upper) =>
          val df = spark.read.format("jdbc")
            .option("url", urlWithUserAndPass)
            .option("dbtable", "TEST.DATETIME")
            .option("partitionColumn", "t")
            .option("lowerBound", lower)
            .option("upperBound", upper)
            .option("numPartitions", 2)
            .load()

          df.logicalPlan match {
            case lr: LogicalRelation if lr.relation.isInstanceOf[JDBCRelation] =>
              val jdbcRelation = lr.relation.asInstanceOf[JDBCRelation]
              val whereClauses = jdbcRelation.parts.map(_.asInstanceOf[JDBCPartition].whereClause)
              assert(whereClauses.toSet === Set(
                s""""T" < '$middle' or "T" is null""",
                s""""T" >= '$middle'"""))
          }
        }
      }
    }
  }

  test("Add exception when isolationLevel is Illegal") {
    val e = intercept[IllegalArgumentException] {
      spark.read.format("jdbc")
        .option("Url", urlWithUserAndPass)
        .option("dbTable", "test.people")
        .option("isolationLevel", "test")
        .load()
    }.getMessage
    assert(e.contains(
      "Invalid value `test` for parameter `isolationLevel`. This can be " +
      "`NONE`, `READ_UNCOMMITTED`, `READ_COMMITTED`, `REPEATABLE_READ` or `SERIALIZABLE`."))
  }

  test("SPARK-28552: Case-insensitive database URLs in JdbcDialect") {
    assert(JdbcDialects.get("jdbc:mysql://localhost/db") === MySQLDialect)
    assert(JdbcDialects.get("jdbc:MySQL://localhost/db") === MySQLDialect)
    assert(JdbcDialects.get("jdbc:postgresql://localhost/db") === PostgresDialect)
    assert(JdbcDialects.get("jdbc:postGresql://localhost/db") === PostgresDialect)
    assert(JdbcDialects.get("jdbc:db2://localhost/db") === DB2Dialect)
    assert(JdbcDialects.get("jdbc:DB2://localhost/db") === DB2Dialect)
    assert(JdbcDialects.get("jdbc:sqlserver://localhost/db") === MsSqlServerDialect)
    assert(JdbcDialects.get("jdbc:sqlServer://localhost/db") === MsSqlServerDialect)
    assert(JdbcDialects.get("jdbc:derby://localhost/db") === DerbyDialect)
    assert(JdbcDialects.get("jdbc:derBy://localhost/db") === DerbyDialect)
    assert(JdbcDialects.get("jdbc:oracle://localhost/db") === OracleDialect)
    assert(JdbcDialects.get("jdbc:Oracle://localhost/db") === OracleDialect)
    assert(JdbcDialects.get("jdbc:teradata://localhost/db") === TeradataDialect)
    assert(JdbcDialects.get("jdbc:Teradata://localhost/db") === TeradataDialect)
  }

  test("SQLContext.jdbc (deprecated)") {
    val sqlContext = spark.sqlContext
    var jdbcDF = sqlContext.jdbc(urlWithUserAndPass, "TEST.PEOPLE")
    checkAnswer(jdbcDF, Row("fred", 1) :: Row("mary", 2) :: Row ("joe 'foo' \"bar\"", 3) :: Nil)

    jdbcDF = sqlContext.jdbc(urlWithUserAndPass, "TEST.PEOPLE", "THEID", 0, 4, 3)
    checkNumPartitions(jdbcDF, 3)
    checkAnswer(jdbcDF, Row("fred", 1) :: Row("mary", 2) :: Row ("joe 'foo' \"bar\"", 3) :: Nil)

    val parts = Array[String]("THEID = 2")
    jdbcDF = sqlContext.jdbc(urlWithUserAndPass, "TEST.PEOPLE", parts)
    checkAnswer(jdbcDF, Row("mary", 2) :: Nil)
  }

  test("SPARK-32364: JDBCOption constructor") {
    val extraOptions = CaseInsensitiveMap[String](Map("UrL" -> "url1", "dBTable" -> "table1"))
    val connectionProperties = new Properties()
    connectionProperties.put("url", "url2")
    connectionProperties.put("dbtable", "table2")

    // connection property should override the options in extraOptions
    val params = extraOptions ++ connectionProperties.asScala
    assert(params.size == 2)
    assert(params.get("uRl").contains("url2"))
    assert(params.get("DbtaBle").contains("table2"))

    // JDBCOptions constructor parameter should overwrite the existing conf
    val options = new JDBCOptions(url, "table3", params)
    assert(options.asProperties.size == 2)
    assert(options.asProperties.get("url") == url)
    assert(options.asProperties.get("dbtable") == "table3")
  }
}
