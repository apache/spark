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
import java.sql.{Connection, Date, Timestamp}
import java.util.Properties

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, ByteType, ShortType, StructType}
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., ibmcom/db2:11.5.4.0):
 * {{{
 *   DB2_DOCKER_IMAGE_NAME=ibmcom/db2:11.5.4.0
 *     ./build/sbt -Pdocker-integration-tests
 *     "testOnly org.apache.spark.sql.jdbc.DB2IntegrationSuite"
 * }}}
 */
@DockerTest
class DB2IntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("DB2_DOCKER_IMAGE_NAME", "ibmcom/db2:11.5.4.0")
    override val env = Map(
      "DB2INST1_PASSWORD" -> "rootpass",
      "LICENSE" -> "accept",
      "DBNAME" -> "foo",
      "ARCHIVE_LOGS" -> "false",
      "AUTOCONFIG" -> "false"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 50000
    override val privileged = true
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:db2://$ip:$port/foo:user=db2inst1;password=rootpass;retrieveMessagesFromServerOnGetMessage=true;" //scalastyle:ignore
  }

  override val connectionTimeout = timeout(3.minutes)

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE TABLE tbl (x INTEGER, y VARCHAR(8))").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement("CREATE TABLE numbers ( small SMALLINT, med INTEGER, big BIGINT, "
      + "deci DECIMAL(31,20), flt FLOAT, dbl DOUBLE, real REAL, "
      + "decflt DECFLOAT, decflt16 DECFLOAT(16), decflt34 DECFLOAT(34))").executeUpdate()
    conn.prepareStatement("INSERT INTO numbers VALUES (17, 77777, 922337203685477580, "
      + "123456745.56789012345000000000, 42.75, 5.4E-70, "
      + "3.4028234663852886e+38, 4.2999, DECFLOAT('9.999999999999999E19', 16), "
      + "DECFLOAT('1234567891234567.123456789123456789', 34))").executeUpdate()

    conn.prepareStatement("CREATE TABLE dates (d DATE, t TIME, ts TIMESTAMP )").executeUpdate()
    conn.prepareStatement("INSERT INTO dates VALUES ('1991-11-09', '13:31:24', "
      + "'2009-02-13 23:31:30')").executeUpdate()

    // TODO: Test locale conversion for strings.
    conn.prepareStatement("CREATE TABLE strings (a CHAR(10), b VARCHAR(10), c CLOB, d BLOB, e XML)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO strings VALUES ('the', 'quick', 'brown', BLOB('fox'),"
      + "'<cinfo cid=\"10\"><name>Kathy</name></cinfo>')").executeUpdate()
  }

  test("Basic test") {
    val df = sqlContext.read.jdbc(jdbcUrl, "tbl", new Properties)
    val rows = df.collect()
    assert(rows.length == 2)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 2)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.String"))
  }

  test("Numeric types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 10)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.Integer"))
    assert(types(2).equals("class java.lang.Long"))
    assert(types(3).equals("class java.math.BigDecimal"))
    assert(types(4).equals("class java.lang.Double"))
    assert(types(5).equals("class java.lang.Double"))
    assert(types(6).equals("class java.lang.Float"))
    assert(types(7).equals("class java.math.BigDecimal"))
    assert(types(8).equals("class java.math.BigDecimal"))
    assert(types(9).equals("class java.math.BigDecimal"))
    assert(rows(0).getInt(0) == 17)
    assert(rows(0).getInt(1) == 77777)
    assert(rows(0).getLong(2) == 922337203685477580L)
    val bd = new BigDecimal("123456745.56789012345000000000")
    assert(rows(0).getAs[BigDecimal](3).equals(bd))
    assert(rows(0).getDouble(4) == 42.75)
    assert(rows(0).getDouble(5) == 5.4E-70)
    assert(rows(0).getFloat(6) == 3.4028234663852886e+38)
    assert(rows(0).getDecimal(7) == new BigDecimal("4.299900000000000000"))
    assert(rows(0).getDecimal(8) == new BigDecimal("99999999999999990000.000000000000000000"))
    assert(rows(0).getDecimal(9) == new BigDecimal("1234567891234567.123456789123456789"))
  }

  test("Date types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 3)
    assert(types(0).equals("class java.sql.Date"))
    assert(types(1).equals("class java.sql.Timestamp"))
    assert(types(2).equals("class java.sql.Timestamp"))
    assert(rows(0).getAs[Date](0).equals(Date.valueOf("1991-11-09")))
    assert(rows(0).getAs[Timestamp](1).equals(Timestamp.valueOf("1970-01-01 13:31:24")))
    assert(rows(0).getAs[Timestamp](2).equals(Timestamp.valueOf("2009-02-13 23:31:30")))
  }

  test("String types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 5)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.String"))
    assert(types(2).equals("class java.lang.String"))
    assert(types(3).equals("class [B"))
    assert(rows(0).getString(0).equals("the       "))
    assert(rows(0).getString(1).equals("quick"))
    assert(rows(0).getString(2).equals("brown"))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](3), Array[Byte](102, 111, 120)))
    assert(rows(0).getString(4).equals("""<cinfo cid="10"><name>Kathy</name></cinfo>"""))
  }

  test("Basic write test") {
    // cast decflt column with precision value of 38 to DB2 max decimal precision value of 31.
    val df1 = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties)
      .selectExpr("small", "med", "big", "deci", "flt", "dbl", "real",
      "cast(decflt as decimal(31, 5)) as decflt")
    val df2 = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
    val df3 = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    df1.write.jdbc(jdbcUrl, "numberscopy", new Properties)
    df2.write.jdbc(jdbcUrl, "datescopy", new Properties)
    df3.write.jdbc(jdbcUrl, "stringscopy", new Properties)
    // spark types that does not have exact matching db2 table types.
    val df4 = sqlContext.createDataFrame(
      sparkContext.parallelize(Seq(Row("1".toShort, "20".toByte, true))),
      new StructType().add("c1", ShortType).add("b", ByteType).add("c3", BooleanType))
    df4.write.jdbc(jdbcUrl, "otherscopy", new Properties)
    val rows = sqlContext.read.jdbc(jdbcUrl, "otherscopy", new Properties).collect()
    assert(rows(0).getInt(0) == 1)
    assert(rows(0).getInt(1) == 20)
    assert(rows(0).getString(2) == "1")
  }

  test("query JDBC option") {
    val expectedResult = Set(
      (42, "fred"),
      (17, "dave")
    ).map { case (x, y) =>
      Row(Integer.valueOf(x), String.valueOf(y))
    }

    val query = "SELECT x, y FROM tbl WHERE x > 10"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", query)
      .load()
    assert(df.collect.toSet === expectedResult)

    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$jdbcUrl', query '$query')
       """.stripMargin.replaceAll("\n", " "))
    assert(sql("select x, y from queryOption").collect.toSet == expectedResult)
  }
}
