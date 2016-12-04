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
import java.sql.{Date, DriverManager, Timestamp}
import java.util.{Calendar, GregorianCalendar, Properties}

import org.h2.jdbc.JdbcSQLException
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation, JdbcUtils}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCSuite extends SparkFunSuite
  with BeforeAndAfter with PrivateMethodTester with SharedSQLContext {
  import testImplicits._

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2")
    override def getCatalystType(
        sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
      Some(StringType)
  }

  before {
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
        |CREATE TEMPORARY TABLE foobar
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE TEMPORARY TABLE fetchtwo
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         ${JDBCOptions.JDBC_BATCH_FETCH_SIZE} '2')
      """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE TEMPORARY TABLE parts
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         partitionColumn 'THEID', lowerBound '1', upperBound '4', numPartitions '3')
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
        |CREATE TEMPORARY TABLE inttypes
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
        |CREATE TEMPORARY TABLE strtypes
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
        |CREATE TEMPORARY TABLE timetypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.TIMETYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))


    conn.prepareStatement("create table test.flttypes (a DOUBLE, b REAL, c DECIMAL(38, 18))"
        ).executeUpdate()
    conn.prepareStatement("insert into test.flttypes values ("
      + "1.0000000000000002220446049250313080847263336181640625, "
      + "1.00000011920928955078125, "
      + "123456789012345.543215432154321)").executeUpdate()
    conn.commit()
    sql(
      s"""
        |CREATE TEMPORARY TABLE flttypes
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
         |CREATE TEMPORARY TABLE nulltypes
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
         |CREATE TEMPORARY TABLE nullparts
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
         |CREATE TEMPORARY TABLE mixedCaseCols
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST."mixedCaseCols"', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    // Untested: IDENTITY, OTHER, UUID, ARRAY, and GEOMETRY types.
  }

  after {
    conn.close()
  }

  // Check whether the tables are fetched in the expected degree of parallelism
  def checkNumPartitions(df: DataFrame, expectedNumPartitions: Int): Unit = {
    val jdbcRelations = df.queryExecution.analyzed.collect {
      case LogicalRelation(r: JDBCRelation, _, _) => r
    }
    assert(jdbcRelations.length == 1)
    assert(jdbcRelations.head.parts.length == expectedNumPartitions,
      s"Expecting a JDBCRelation with $expectedNumPartitions partitions, but got:`$jdbcRelations`")
  }

  test("SELECT *") {
    assert(sql("SELECT * FROM foobar").collect().size === 3)
  }

  test("SELECT * WHERE (simple predicates)") {
    def checkPushdown(df: DataFrame): DataFrame = {
      val parentPlan = df.queryExecution.executedPlan
      // Check if SparkPlan Filter is removed in a physical plan and
      // the plan only has PhysicalRDD to scan JDBCRelation.
      assert(parentPlan.isInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec])
      val node = parentPlan.asInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec]
      assert(node.child.isInstanceOf[org.apache.spark.sql.execution.DataSourceScanExec])
      assert(node.child.asInstanceOf[DataSourceScanExec].nodeName.contains("JDBCRelation"))
      df
    }
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
    val df1 = sql("SELECT * FROM foobar WHERE NOT (THEID != 2 AND NAME != 'mary')")
    val df2 = sql("SELECT * FROM foobar WHERE NOT (THEID != 2) OR NOT (NAME != 'mary')")
    assert(df1.collect.toSet === Set(Row("mary", 2)))
    assert(df2.collect.toSet === Set(Row("mary", 2)))

    def checkNotPushdown(df: DataFrame): DataFrame = {
      val parentPlan = df.queryExecution.executedPlan
      // Check if SparkPlan Filter is not removed in a physical plan because JDBCRDD
      // cannot compile given predicates.
      assert(parentPlan.isInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec])
      val node = parentPlan.asInstanceOf[org.apache.spark.sql.execution.WholeStageCodegenExec]
      assert(node.child.isInstanceOf[org.apache.spark.sql.execution.FilterExec])
      df
    }
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

  test("Register JDBC query with renamed fields") {
    // Regression test for bug SPARK-7345
    sql(
      s"""
        |CREATE TEMPORARY TABLE renamed
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

  test("Basic API with illegal fetchsize") {
    val properties = new Properties()
    properties.setProperty(JDBCOptions.JDBC_BATCH_FETCH_SIZE, "-1")
    val e = intercept[IllegalArgumentException] {
      spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", properties).collect()
    }.getMessage
    assert(e.contains("Invalid value `-1` for parameter `fetchsize`"))
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
    val rows = spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties()).collect()
    spark.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties())
      .cache().createOrReplaceTempView("mycached_date")
    val cachedRows = sql("select * from mycached_date").collect()
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
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
        |CREATE TEMPORARY TABLE hack
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
          |CREATE TEMPORARY TABLE abc
          |USING org.apache.spark.sql.jdbc
          |OPTIONS (url '$url', dbtable '(SELECT _ROWID_ FROM test.people)',
          |         user 'testUser', password 'testPass')
        """.stripMargin.replaceAll("\n", " "))
    }
  }

  test("Remap types via JdbcDialects") {
    JdbcDialects.registerDialect(testH2Dialect)
    val df = spark.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", new Properties())
    assert(df.schema.filter(_.dataType != org.apache.spark.sql.types.StringType).isEmpty)
    val rows = df.collect()
    assert(rows(0).get(0).isInstanceOf[String])
    assert(rows(0).get(1).isInstanceOf[String])
    JdbcDialects.unregisterDialect(testH2Dialect)
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
    val compileFilter = PrivateMethod[Option[String]]('compileFilter)
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
    JdbcDialects.registerDialect(testH2Dialect)
    JdbcDialects.unregisterDialect(testH2Dialect)
    assert(JdbcDialects.get(urlWithUserAndPass) == NoopDialect)
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
    }, testH2Dialect))
    assert(agg.canHandle("jdbc:h2:xxx"))
    assert(!agg.canHandle("jdbc:h2"))
    assert(agg.getCatalystType(0, "", 1, null) === Some(LongType))
    assert(agg.getCatalystType(1, "", 1, null) === Some(StringType))
  }

  test("DB2Dialect type mapping") {
    val db2Dialect = JdbcDialects.get("jdbc:db2://127.0.0.1/db")
    assert(db2Dialect.getJDBCType(StringType).map(_.databaseTypeDefinition).get == "CLOB")
    assert(db2Dialect.getJDBCType(BooleanType).map(_.databaseTypeDefinition).get == "CHAR(1)")
  }

  test("PostgresDialect type mapping") {
    val Postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    assert(Postgres.getCatalystType(java.sql.Types.OTHER, "json", 1, null) === Some(StringType))
    assert(Postgres.getCatalystType(java.sql.Types.OTHER, "jsonb", 1, null) === Some(StringType))
    assert(Postgres.getJDBCType(FloatType).map(_.databaseTypeDefinition).get == "FLOAT4")
    assert(Postgres.getJDBCType(DoubleType).map(_.databaseTypeDefinition).get == "FLOAT8")
    val errMsg = intercept[IllegalArgumentException] {
      Postgres.getJDBCType(ByteType)
    }
    assert(errMsg.getMessage contains "Unsupported type in postgresql: ByteType")
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

  test("test credentials in the properties are not in plan output") {
    val df = sql("SELECT * FROM parts")
    val explain = ExplainCommand(df.queryExecution.logical, extended = true)
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
    val explain = ExplainCommand(df.queryExecution.logical, extended = true)
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

        val explain = ExplainCommand(df.queryExecution.logical, extended = true)
        spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach { r =>
          assert(!r.toString.contains(password))
        }

        sql(s"DESC FORMATTED $tableName").collect().foreach { r =>
          assert(!r.toString().contains(password))
        }

        sql(s"DESC EXTENDED $tableName").collect().foreach { r =>
          assert(!r.toString().contains(password))
        }
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
    val schema = JdbcUtils.schemaString(df.schema, "jdbc:mysql://localhost:3306/temp")
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
    assert(new JDBCOptions(new CaseInsensitiveMap(parameters)).asConnectionProperties.isEmpty)
  }
}
