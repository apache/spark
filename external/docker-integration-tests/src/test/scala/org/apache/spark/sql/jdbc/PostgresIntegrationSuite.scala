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

import java.sql.Connection
import java.util.Properties
import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{ArrayType, DecimalType, FloatType, ShortType}
import org.apache.spark.tags.DockerTest

@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationSuite {
  import testImplicits._
  override val db = new DatabaseOnDocker {
    override val imageName = "postgres:9.5.4"
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
    override def getStartupProcessName: Option[String] = None
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.setCatalog("foo")
    conn.prepareStatement("CREATE TYPE enum_type AS ENUM ('d1', 'd2')").executeUpdate()
    conn.prepareStatement("CREATE TABLE bar (c0 text, c1 integer, c2 double precision, c3 bigint, "
      + "c4 bit(1), c5 bit(10), c6 bytea, c7 boolean, c8 inet, c9 cidr, "
      + "c10 integer[], c11 text[], c12 real[], c13 numeric(2,2)[], c14 enum_type, "
      + "c15 float4, c16 smallint)").executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES ('hello', 42, 1.25, 123456789012345, B'0', "
      + "B'1000100101', E'\\\\xDEADBEEF', true, '172.16.0.42', '192.168.0.0/16', "
      + """'{1, 2}', '{"a", null, "b"}', '{0.11, 0.22}', '{0.11, 0.22}', 'd1', 1.01, 1)"""
    ).executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES (null, null, null, null, null, "
      + "null, null, null, null, null, "
      + "null, null, null, null, null, null, null)"
    ).executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT0 " +
      "(c1 INTEGER, c2 INTEGER, c3 INTEGER, primary key(c1))").executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT0 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT1 " +
      "(c1 INTEGER, c2 INTEGER, c3 INTEGER, primary key(c1))").executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT1 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT2 " +
      "(c1 INTEGER, c2 INTEGER, c3 INTEGER, primary key(c1, c2))").executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT2 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT3 " +
      "(c1 INTEGER, c2 INTEGER, c3 INTEGER, primary key(c1))").executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT3 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
    conn.prepareStatement("CREATE TABLE upsertT4 " +
      "(c1 INTEGER, c2 INTEGER, c3 INTEGER, primary key(c1))").executeUpdate()
    conn.prepareStatement("INSERT INTO upsertT4 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)")
      .executeUpdate()
  }

  test("Type mapping for various types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    val rows = df.collect().sortBy(_.toString())
    assert(rows.length == 2)
    // Test the types, and values using the first row.
    val types = rows(0).toSeq.map(x => x.getClass)
    assert(types.length == 17)
    assert(classOf[String].isAssignableFrom(types(0)))
    assert(classOf[java.lang.Integer].isAssignableFrom(types(1)))
    assert(classOf[java.lang.Double].isAssignableFrom(types(2)))
    assert(classOf[java.lang.Long].isAssignableFrom(types(3)))
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(4)))
    assert(classOf[Array[Byte]].isAssignableFrom(types(5)))
    assert(classOf[Array[Byte]].isAssignableFrom(types(6)))
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(7)))
    assert(classOf[String].isAssignableFrom(types(8)))
    assert(classOf[String].isAssignableFrom(types(9)))
    assert(classOf[Seq[Int]].isAssignableFrom(types(10)))
    assert(classOf[Seq[String]].isAssignableFrom(types(11)))
    assert(classOf[Seq[Double]].isAssignableFrom(types(12)))
    assert(classOf[Seq[BigDecimal]].isAssignableFrom(types(13)))
    assert(classOf[String].isAssignableFrom(types(14)))
    assert(classOf[java.lang.Float].isAssignableFrom(types(15)))
    assert(classOf[java.lang.Short].isAssignableFrom(types(16)))
    assert(rows(0).getString(0).equals("hello"))
    assert(rows(0).getInt(1) == 42)
    assert(rows(0).getDouble(2) == 1.25)
    assert(rows(0).getLong(3) == 123456789012345L)
    assert(!rows(0).getBoolean(4))
    // BIT(10)'s come back as ASCII strings of ten ASCII 0's and 1's...
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](5),
      Array[Byte](49, 48, 48, 48, 49, 48, 48, 49, 48, 49)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](6),
      Array[Byte](0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte)))
    assert(rows(0).getBoolean(7))
    assert(rows(0).getString(8) == "172.16.0.42")
    assert(rows(0).getString(9) == "192.168.0.0/16")
    assert(rows(0).getSeq(10) == Seq(1, 2))
    assert(rows(0).getSeq(11) == Seq("a", null, "b"))
    assert(rows(0).getSeq(12).toSeq == Seq(0.11f, 0.22f))
    assert(rows(0).getSeq(13) == Seq("0.11", "0.22").map(BigDecimal(_).bigDecimal))
    assert(rows(0).getString(14) == "d1")
    assert(rows(0).getFloat(15) == 1.01f)
    assert(rows(0).getShort(16) == 1)

    // Test reading null values using the second row.
    assert(0.until(16).forall(rows(1).isNullAt(_)))
  }

  test("Basic write test") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    // Test only that it doesn't crash.
    df.write.jdbc(jdbcUrl, "public.barcopy", new Properties)
    // Test that written numeric type has same DataType as input
    assert(sqlContext.read.jdbc(jdbcUrl, "public.barcopy", new Properties).schema(13).dataType ==
      ArrayType(DecimalType(2, 2), true))
    // Test write null values.
    df.select(df.queryExecution.analyzed.output.map { a =>
      Column(Literal.create(null, a.dataType)).as(a.name)
    }: _*).write.jdbc(jdbcUrl, "public.barcopy2", new Properties)
  }

  test("Creating a table with shorts and floats") {
    sqlContext.createDataFrame(Seq((1.0f, 1.toShort)))
      .write.jdbc(jdbcUrl, "shortfloat", new Properties)
    val schema = sqlContext.read.jdbc(jdbcUrl, "shortfloat", new Properties).schema
    assert(schema(0).dataType == FloatType)
    assert(schema(1).dataType == ShortType)
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
      .option("upsert", true).option("upsertConditionColumn", "c1")
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
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertConditionColumn", "C11")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    }.getMessage
    assert(m.contains("column C11 not found"))

    val n = intercept[java.sql.SQLException] {
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertConditionColumn", "C11")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    }.getMessage
    assert(n.contains("column C11 not found"))

    val o = intercept[org.apache.spark.SparkException] {
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertconditionColumn", "c2")
      .jdbc(jdbcUrl, "upsertT1", new Properties)
    }.getMessage
    assert(o.contains("there is no unique or exclusion constraint matching the ON CONFLICT"))
  }

  test("upsert with Append without existing table") {
    val df1 = Seq((1, 3), (2, 5)).toDF("c1", "c2")
    df1.write.mode(SaveMode.Append).option("upsert", true).option("upsertConditionColumn", "c1")
      .jdbc(jdbcUrl, "upsertT", new Properties)
    val df2 = spark.read.jdbc(jdbcUrl, "upsertT", new Properties)
    assert(df2.count() == 2)
    assert(df2.filter("C1=1").collect.head.get(1) == 3)

    // table upsertT create without primary key or unique constraints, it will throw the exception
    val df3 = Seq((1, 4)).toDF("c1", "c2")
    val p = intercept[org.apache.spark.SparkException] {
    df3.write.mode(SaveMode.Append).option("upsert", true).option("upsertConditionColumn", "c1")
      .jdbc(jdbcUrl, "upsertT", new Properties)
    }.getMessage
    assert(p.contains("there is no unique or exclusion constraint matching the ON CONFLICT specification"))
  }

  test("Upsert & Append test  -- matching one column") {
    val df1 = spark.read.jdbc(jdbcUrl, "upsertT3", new Properties())
    assert(df1.filter("c1=1").collect.head.getInt(1) == 2)
    assert(df1.filter("c1=1").collect.head.getInt(2) == 3)
    assert(df1.filter("c1=4").collect.size == 0)
    // update Row(1, 2, 3) to (1, 3, 6) and insert new Row(4, 5, 6)
    val df2 = Seq((1, 3, 6), (4, 5, 6)).toDF("c1", "c2", "c3")
    // condition on one column
    df2.write.mode(SaveMode.Append)
      .option("upsert", true).option("upsertConditionColumn", "c1").option("upsertUpdateColumn", "c2, c3")
      .jdbc(jdbcUrl, "upsertT3", new Properties)
    val df3 = spark.read.jdbc(jdbcUrl, "upsertT3", new Properties())
    assert(df3.filter("c1=1").collect.head.getInt(1) == 3)
    assert(df3.filter("c1=1").collect.head.getInt(2) == 6)
    assert(df3.filter("c1=4").collect.size == 1)
  }

  test("Upsert & Append test -- matching two columns") {
    val df1 = spark.read.jdbc(jdbcUrl, "upsertT2", new Properties())
    assert(df1.filter("c1=1").collect.head.getInt(1) == 2)
    assert(df1.filter("c1=1").collect.head.getInt(2) == 3)
    // update Row(2, 3, 4) to Row(2, 3, 10) that matches 2 columns
    val df2 = Seq((2, 3, 10)).toDF("c1", "c2", "c3")
    df2.write.mode(SaveMode.Append)
      .option("upsert", true).option("upsertConditionColumn", "c1, c2")
      .jdbc(jdbcUrl, "upsertT2", new Properties)
    val df3 = spark.read.jdbc(jdbcUrl, "upsertT2", new Properties())
    assert(df3.filter("c1=2").collect.head.getInt(2) == 10)
  }
}
