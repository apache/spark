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
import java.sql.DriverManager
import java.util.{Calendar, GregorianCalendar, Properties}

import org.h2.jdbc.JdbcSQLException
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCSuite extends SparkFunSuite with BeforeAndAfter with SharedSQLContext {
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
        |         fetchSize '2')
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

    // Untested: IDENTITY, OTHER, UUID, ARRAY, and GEOMETRY types.
  }

  after {
    conn.close()
  }

  test("SELECT *") {
    assert(sql("SELECT * FROM foobar").collect().size === 3)
  }

  test("SELECT * WHERE (simple predicates)") {
    assert(sql("SELECT * FROM foobar WHERE THEID < 1").collect().size === 0)
    assert(sql("SELECT * FROM foobar WHERE THEID != 2").collect().size === 2)
    assert(sql("SELECT * FROM foobar WHERE THEID = 1").collect().size === 1)
    assert(sql("SELECT * FROM foobar WHERE NAME = 'fred'").collect().size === 1)
    assert(sql("SELECT * FROM foobar WHERE NAME > 'fred'").collect().size === 2)
    assert(sql("SELECT * FROM foobar WHERE NAME != 'fred'").collect().size === 2)
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

  test("SELECT first field when fetchSize is two") {
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

  test("SELECT second field when fetchSize is two") {
    val ids = sql("SELECT THEID FROM fetchtwo").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }

  test("SELECT * partitioned") {
    assert(sql("SELECT * FROM parts").collect().size == 3)
  }

  test("SELECT WHERE (simple predicates) partitioned") {
    assert(sql("SELECT * FROM parts WHERE THEID < 1").collect().size === 0)
    assert(sql("SELECT * FROM parts WHERE THEID != 2").collect().size === 2)
    assert(sql("SELECT THEID FROM parts WHERE THEID = 1").collect().size === 1)
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
    assert(ctx.read.jdbc(
      urlWithUserAndPass, "TEST.PEOPLE", new Properties).collect().length === 3)
  }

  test("Basic API with FetchSize") {
    val properties = new Properties
    properties.setProperty("fetchSize", "2")
    assert(ctx.read.jdbc(
      urlWithUserAndPass, "TEST.PEOPLE", properties).collect().length === 3)
  }

  test("Partitioning via JDBCPartitioningInfo API") {
    assert(
      ctx.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", "THEID", 0, 4, 3, new Properties)
      .collect().length === 3)
  }

  test("Partitioning via list-of-where-clauses API") {
    val parts = Array[String]("THEID < 2", "THEID >= 2")
    assert(ctx.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", parts, new Properties)
      .collect().length === 3)
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
    val rows = ctx.read.jdbc(
      urlWithUserAndPass, "TEST.TIMETYPES", new Properties).collect()
    val cachedRows = ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties)
      .cache().collect()
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    assert(rows(1).getAs[java.sql.Date](1) === null)
    assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
  }

  test("test DATE types in cache") {
    val rows = ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties).collect()
    ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties)
      .cache().registerTempTable("mycached_date")
    val cachedRows = sql("select * from mycached_date").collect()
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
  }

  test("test types for null value") {
    val rows = ctx.read.jdbc(
      urlWithUserAndPass, "TEST.NULLTYPES", new Properties).collect()
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
    val df = ctx.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", new Properties)
    assert(df.schema.filter(_.dataType != org.apache.spark.sql.types.StringType).isEmpty)
    val rows = df.collect()
    assert(rows(0).get(0).isInstanceOf[String])
    assert(rows(0).get(1).isInstanceOf[String])
    JdbcDialects.unregisterDialect(testH2Dialect)
  }

  test("Default jdbc dialect registration") {
    assert(JdbcDialects.get("jdbc:mysql://127.0.0.1/db") == MySQLDialect)
    assert(JdbcDialects.get("jdbc:postgresql://127.0.0.1/db") == PostgresDialect)
    assert(JdbcDialects.get("test.invalid") == NoopDialect)
  }

  test("quote column names by jdbc dialect") {
    val MySQL = JdbcDialects.get("jdbc:mysql://127.0.0.1/db")
    val Postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")

    val columns = Seq("abc", "key")
    val MySQLColumns = columns.map(MySQL.quoteIdentifier(_))
    val PostgresColumns = columns.map(Postgres.quoteIdentifier(_))
    assert(MySQLColumns === Seq("`abc`", "`key`"))
    assert(PostgresColumns === Seq(""""abc"""", """"key""""))
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
}
