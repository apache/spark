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
import org.apache.spark.sql.SaveMode

import org.apache.spark.tags.DockerTest

@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationSuite {
  import testImplicits._
  override val db = new DatabaseOnDocker {
    override val imageName = "mysql:5.7.9"
    override val env = Map(
      "MYSQL_ROOT_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 3306
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass"
    override def getStartupProcessName: Option[String] = None
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.prepareStatement("CREATE TABLE tbl (x INTEGER, y TEXT(8))").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement("CREATE TABLE numbers (onebit BIT(1), tenbits BIT(10), "
      + "small SMALLINT, med MEDIUMINT, nor INT, big BIGINT, deci DECIMAL(40,20), flt FLOAT, "
      + "dbl DOUBLE)").executeUpdate()
    conn.prepareStatement("INSERT INTO numbers VALUES (b'0', b'1000100101', "
      + "17, 77777, 123456789, 123456789012345, 123456789012345.123456789012345, "
      + "42.75, 1.0000000000000002)").executeUpdate()

    conn.prepareStatement("CREATE TABLE dates (d DATE, t TIME, dt DATETIME, ts TIMESTAMP, "
      + "yr YEAR)").executeUpdate()
    conn.prepareStatement("INSERT INTO dates VALUES ('1991-11-09', '13:31:24', "
      + "'1996-01-01 01:23:45', '2009-02-13 23:31:30', '2001')").executeUpdate()

    // TODO: Test locale conversion for strings.
    conn.prepareStatement("CREATE TABLE strings (a CHAR(10), b VARCHAR(10), c TINYTEXT, "
      + "d TEXT, e MEDIUMTEXT, f LONGTEXT, g BINARY(4), h VARBINARY(10), i BLOB)"
    ).executeUpdate()
    conn.prepareStatement("INSERT INTO strings VALUES ('the', 'quick', 'brown', 'fox', " +
      "'jumps', 'over', 'the', 'lazy', 'dog')").executeUpdate()

    conn.prepareStatement("CREATE TABLE upsertT0 (c1 INTEGER primary key, c2 INTEGER, c3 INTEGER)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT0 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT1 (c1 INTEGER primary key, c2 INTEGER, c3 INTEGER)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT1 VALUES (1, 2, 3), (2, 3, 4)")
      .executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT2 (c1 INTEGER, c2 INTEGER, c3 INTEGER, " +
      "primary key(c1, c2))").executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT2 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
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
    assert(types.length == 9)
    assert(types(0).equals("class java.lang.Boolean"))
    assert(types(1).equals("class java.lang.Long"))
    assert(types(2).equals("class java.lang.Integer"))
    assert(types(3).equals("class java.lang.Integer"))
    assert(types(4).equals("class java.lang.Integer"))
    assert(types(5).equals("class java.lang.Long"))
    assert(types(6).equals("class java.math.BigDecimal"))
    assert(types(7).equals("class java.lang.Double"))
    assert(types(8).equals("class java.lang.Double"))
    assert(rows(0).getBoolean(0) == false)
    assert(rows(0).getLong(1) == 0x225)
    assert(rows(0).getInt(2) == 17)
    assert(rows(0).getInt(3) == 77777)
    assert(rows(0).getInt(4) == 123456789)
    assert(rows(0).getLong(5) == 123456789012345L)
    val bd = new BigDecimal("123456789012345.12345678901234500000")
    assert(rows(0).getAs[BigDecimal](6).equals(bd))
    assert(rows(0).getDouble(7) == 42.75)
    assert(rows(0).getDouble(8) == 1.0000000000000002)
  }

  test("Date types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 5)
    assert(types(0).equals("class java.sql.Date"))
    assert(types(1).equals("class java.sql.Timestamp"))
    assert(types(2).equals("class java.sql.Timestamp"))
    assert(types(3).equals("class java.sql.Timestamp"))
    assert(types(4).equals("class java.sql.Date"))
    assert(rows(0).getAs[Date](0).equals(Date.valueOf("1991-11-09")))
    assert(rows(0).getAs[Timestamp](1).equals(Timestamp.valueOf("1970-01-01 13:31:24")))
    assert(rows(0).getAs[Timestamp](2).equals(Timestamp.valueOf("1996-01-01 01:23:45")))
    assert(rows(0).getAs[Timestamp](3).equals(Timestamp.valueOf("2009-02-13 23:31:30")))
    assert(rows(0).getAs[Date](4).equals(Date.valueOf("2001-01-01")))
  }

  test("String types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 9)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.String"))
    assert(types(2).equals("class java.lang.String"))
    assert(types(3).equals("class java.lang.String"))
    assert(types(4).equals("class java.lang.String"))
    assert(types(5).equals("class java.lang.String"))
    assert(types(6).equals("class [B"))
    assert(types(7).equals("class [B"))
    assert(types(8).equals("class [B"))
    assert(rows(0).getString(0).equals("the"))
    assert(rows(0).getString(1).equals("quick"))
    assert(rows(0).getString(2).equals("brown"))
    assert(rows(0).getString(3).equals("fox"))
    assert(rows(0).getString(4).equals("jumps"))
    assert(rows(0).getString(5).equals("over"))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](6), Array[Byte](116, 104, 101, 0)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](7), Array[Byte](108, 97, 122, 121)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](8), Array[Byte](100, 111, 103)))
  }

  test("Basic write test") {
    val df1 = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties)
    val df2 = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
    val df3 = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    df1.write.jdbc(jdbcUrl, "numberscopy", new Properties)
    df2.write.jdbc(jdbcUrl, "datescopy", new Properties)
    df3.write.jdbc(jdbcUrl, "stringscopy", new Properties)
  }

  test("upsert with Append without existing table") {
    val df1 = Seq((1, 3), (2, 5)).toDF("c1", "c2")
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertUpdateColumn", "c1")
      .jdbc(jdbcUrl, "upsertT", new Properties)
    val df2 = spark.read.jdbc(jdbcUrl, "upsertT", new Properties)
    assert(df2.count() == 2)
    assert(df2.filter("C1=1").collect.head.get(1) == 3)

    // table upsertT create without primary key or unique constraints, it will do the insert
    val df3 = Seq((1, 4)).toDF("c1", "c2")
    df3.write.mode(SaveMode.Append).option("upsert", true).option("upsertUpdateColumn", "c1")
      .jdbc(jdbcUrl, "upsertT", new Properties)
    assert(spark.read.jdbc(jdbcUrl, "upsertT", new Properties).filter("c1=1").count() == 2)
  }

  test("Upsert and OverWrite mode") {
    //existing table has these rows
    //(1, 2, 3), (2, 3, 4), (3, 4, 5)
    val df1 = spark.read.jdbc(jdbcUrl, "upsertT0", new Properties())
    assert(df1.filter("c1=1").collect.head.getInt(1) == 2)
    assert(df1.filter("c1=1").collect.head.getInt(2) == 3)
    assert(df1.filter("c1=2").collect.head.getInt(1) == 3)
    assert(df1.filter("c1=2").collect.head.getInt(2) == 4)
    val df2 = Seq((1, 3, 6), (2, 5, 6)).toDF("c1", "c2", "c3")
    // it will do the Overwrite, not upsert
    df2.write.mode(SaveMode.Overwrite)
      .option("upsert", true).option("upsertUpdateColumn", "c2, c3")
      .jdbc(jdbcUrl, "upsertT0", new Properties)
    val df3 = spark.read.jdbc(jdbcUrl, "upsertT0", new Properties())
    assert(df3.filter("c1=1").collect.head.getInt(1) == 3)
    assert(df3.filter("c1=1").collect.head.getInt(2) == 6)
    assert(df3.filter("c1=2").collect.head.getInt(1) == 5)
    assert(df3.filter("c1=2").collect.head.getInt(2) == 6)
    assert(df3.filter("c1=3").collect.size == 0)
  }

  test("upsert with Append and negative option values") {
    val df1 = Seq((1, 3, 6), (2, 5, 6)).toDF("c1", "c2", "c3")
    val m = intercept[java.sql.SQLException] {
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertUpdateColumn", "C11")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    }.getMessage
    assert(m.contains("column C11 not found"))

    val n = intercept[java.sql.SQLException] {
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertUpdateColumn", "C11")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    }.getMessage
    assert(n.contains("column C11 not found"))
  }

  test("Upsert and Append mode -- data matching one column") {
    //existing table has these rows
    //(1, 2, 3), (2, 3, 4)
    val df1 = spark.read.jdbc(jdbcUrl, "upsertT1", new Properties())
    assert(df1.count() == 2)
    assert(df1.filter("c1=1").collect.head.getInt(1) == 2)
    assert(df1.filter("c1=1").collect.head.getInt(2) == 3)
    assert(df1.filter("c1=2").collect.head.getInt(1) == 3)
    assert(df1.filter("c1=2").collect.head.getInt(2) == 4)
    val df2 = Seq((1, 4, 7), (2, 6, 8)).toDF("c1", "c2", "c3")
    df2.write.mode(SaveMode.Append)
      .option("upsert", true).option("upsertUpdateColumn", "c2, c3")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    val df3 = spark.read.jdbc(jdbcUrl, "upsertT1", new Properties())
    assert(df3.count() == 2)
    assert(df3.filter("c1=1").collect.head.getInt(1) == 4)
    assert(df3.filter("c1=1").collect.head.getInt(2) == 7)
    assert(df3.filter("c1=2").collect.head.getInt(1) == 6)
    assert(df3.filter("c1=2").collect.head.getInt(2) == 8)
    // turn upsert off, it will do the insert the row with duplicate key, and it will get nullPointerException
    val df4 = Seq((1, 5, 9)).toDF("c1", "c2", "c3")
    val n = intercept[org.apache.spark.SparkException] {
    df4.write.mode(SaveMode.Append).option("upsert", false).option("upsertUpdateColumn", "C11")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    }.getMessage
    assert(n.contains("Duplicate entry '1' for key 'PRIMARY'"))
  }

  test("Upsert and Append mode -- data matching two columns") {
    // table has these rows: (1, 2, 3), (2, 3, 4), (3, 4, 5)
    // update Row(2, 3, 4) to Row(2, 3, 10) that matches 2 columns
    val df1 = spark.read.jdbc(jdbcUrl, "upsertT2", new Properties())
    assert(df1.count() == 3)
    assert(df1.filter("c1=1").collect.head.getInt(1) == 2)
    assert(df1.filter("c1=1").collect.head.getInt(2) == 3)
    assert(df1.filter("c1=2").collect.head.getInt(1) == 3)
    assert(df1.filter("c1=2").collect.head.getInt(2) == 4)

    val df2 = Seq((2, 3, 10)).toDF("c1", "c2", "c3")
    df2.write.mode(SaveMode.Append)
      .option("upsert", true).option("upsertUpdateColumn", "c3")
      .jdbc(jdbcUrl, "upsertT2", new Properties)

    val df3 = spark.read.jdbc(jdbcUrl, "upsertT2", new Properties())
    assert(df3.count() == 3)
    assert(df3.filter("c1=2").collect.head.getInt(1) == 3)
    assert(df3.filter("c1=2").collect.head.getInt(2) == 10)
  }
}
