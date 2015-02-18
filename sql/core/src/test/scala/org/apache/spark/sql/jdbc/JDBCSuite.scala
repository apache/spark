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
import org.apache.spark.sql.test._
import org.scalatest.{FunSuite, BeforeAndAfter}
import java.sql.DriverManager
import TestSQLContext._

class JDBCSuite extends FunSuite with BeforeAndAfter {
  val url = "jdbc:h2:mem:testdb0"
  var conn: java.sql.Connection = null

  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  before {
    Class.forName("org.h2.Driver")
    conn = DriverManager.getConnection(url)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement("create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('joe', 3)").executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE TEMPORARY TABLE foobar
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE')
      """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE TEMPORARY TABLE parts
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE',
        |partitionColumn 'THEID', lowerBound '1', upperBound '4', numPartitions '3')
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
        |OPTIONS (url '$url', dbtable 'TEST.INTTYPES')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.strtypes (a BINARY(20), b VARCHAR(20), "
      + "c VARCHAR_IGNORECASE(20), d CHAR(20), e BLOB, f CLOB)").executeUpdate()
    var stmt = conn.prepareStatement("insert into test.strtypes values (?, ?, ?, ?, ?, ?)")
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
        |OPTIONS (url '$url', dbtable 'TEST.STRTYPES')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.timetypes (a TIME, b DATE, c TIMESTAMP)"
        ).executeUpdate()
    conn.prepareStatement("insert into test.timetypes values ('12:34:56', "
      + "'1996-01-01', '2002-02-20 11:22:33.543543543')").executeUpdate()
    conn.commit()
    sql(
      s"""
        |CREATE TEMPORARY TABLE timetypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.TIMETYPES')
      """.stripMargin.replaceAll("\n", " "))


    conn.prepareStatement("create table test.flttypes (a DOUBLE, b REAL, c DECIMAL(40, 20))"
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
        |OPTIONS (url '$url', dbtable 'TEST.FLTTYPES')
      """.stripMargin.replaceAll("\n", " "))

    // Untested: IDENTITY, OTHER, UUID, ARRAY, and GEOMETRY types.
  }

  after {
    conn.close()
  }

  test("SELECT *") {
    assert(sql("SELECT * FROM foobar").collect().size == 3)
  }

  test("SELECT * WHERE (simple predicates)") {
    assert(sql("SELECT * FROM foobar WHERE THEID < 1").collect().size == 0)
    assert(sql("SELECT * FROM foobar WHERE THEID != 2").collect().size == 2)
    assert(sql("SELECT * FROM foobar WHERE THEID = 1").collect().size == 1)
  }

  test("SELECT first field") {
    val names = sql("SELECT NAME FROM foobar").collect().map(x => x.getString(0)).sortWith(_ < _)
    assert(names.size == 3)
    assert(names(0).equals("fred"))
    assert(names(1).equals("joe"))
    assert(names(2).equals("mary"))
  }

  test("SELECT second field") {
    val ids = sql("SELECT THEID FROM foobar").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size == 3)
    assert(ids(0) == 1)
    assert(ids(1) == 2)
    assert(ids(2) == 3)
  }

  test("SELECT * partitioned") {
    assert(sql("SELECT * FROM parts").collect().size == 3)
  }

  test("SELECT WHERE (simple predicates) partitioned") {
    assert(sql("SELECT * FROM parts WHERE THEID < 1").collect().size == 0)
    assert(sql("SELECT * FROM parts WHERE THEID != 2").collect().size == 2)
    assert(sql("SELECT THEID FROM parts WHERE THEID = 1").collect().size == 1)
  }

  test("SELECT second field partitioned") {
    val ids = sql("SELECT THEID FROM parts").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size == 3)
    assert(ids(0) == 1)
    assert(ids(1) == 2)
    assert(ids(2) == 3)
  }

  test("Basic API") {
    assert(TestSQLContext.jdbc(url, "TEST.PEOPLE").collect.size == 3)
  }

  test("Partitioning via JDBCPartitioningInfo API") {
    assert(TestSQLContext.jdbc(url, "TEST.PEOPLE", "THEID", 0, 4, 3).collect.size == 3)
  }

  test("Partitioning via list-of-where-clauses API") {
    val parts = Array[String]("THEID < 2", "THEID >= 2")
    assert(TestSQLContext.jdbc(url, "TEST.PEOPLE", parts).collect.size == 3)
  }

  test("H2 integral types") {
    val rows = sql("SELECT * FROM inttypes WHERE A IS NOT NULL").collect()
    assert(rows.size == 1)
    assert(rows(0).getInt(0) == 1)
    assert(rows(0).getBoolean(1) == false)
    assert(rows(0).getInt(2) == 3)
    assert(rows(0).getInt(3) == 4)
    assert(rows(0).getLong(4) == 1234567890123L)
  }

  test("H2 null entries") {
    val rows = sql("SELECT * FROM inttypes WHERE A IS NULL").collect()
    assert(rows.size == 1)
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
    assert(rows(0).getAs[java.sql.Timestamp](0).getHours == 12)
    assert(rows(0).getAs[java.sql.Timestamp](0).getMinutes == 34)
    assert(rows(0).getAs[java.sql.Timestamp](0).getSeconds == 56)
    assert(rows(0).getAs[java.sql.Date](1).getYear == 96)
    assert(rows(0).getAs[java.sql.Date](1).getMonth == 0)
    assert(rows(0).getAs[java.sql.Date](1).getDate == 1)
    assert(rows(0).getAs[java.sql.Timestamp](2).getYear == 102)
    assert(rows(0).getAs[java.sql.Timestamp](2).getMonth == 1)
    assert(rows(0).getAs[java.sql.Timestamp](2).getDate == 20)
    assert(rows(0).getAs[java.sql.Timestamp](2).getHours == 11)
    assert(rows(0).getAs[java.sql.Timestamp](2).getMinutes == 22)
    assert(rows(0).getAs[java.sql.Timestamp](2).getSeconds == 33)
    assert(rows(0).getAs[java.sql.Timestamp](2).getNanos == 543543543)
  }

  test("H2 floating-point types") {
    val rows = sql("SELECT * FROM flttypes").collect()
    assert(rows(0).getDouble(0) == 1.00000000000000022) // Yes, I meant ==.
    assert(rows(0).getDouble(1) == 1.00000011920928955) // Yes, I meant ==.
    assert(rows(0).getAs[BigDecimal](2)
        .equals(new BigDecimal("123456789012345.54321543215432100000")))
  }


  test("SQL query as table name") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE hack
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable '(SELECT B, B*B FROM TEST.FLTTYPES)')
      """.stripMargin.replaceAll("\n", " "))
    val rows = sql("SELECT * FROM hack").collect()
    assert(rows(0).getDouble(0) == 1.00000011920928955) // Yes, I meant ==.
    // For some reason, H2 computes this square incorrectly...
    assert(math.abs(rows(0).getDouble(1) - 1.00000023841859331) < 1e-12)
  }
}
