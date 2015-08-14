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
import java.sql.{Date, Timestamp}
import java.util.Properties

import org.scalatest.BeforeAndAfterAll

import com.spotify.docker.client.{ImageNotFoundException, DockerClient}
import com.spotify.docker.client.messages.ContainerConfig

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class MySQLDatabase {
  val docker: DockerClient = DockerClientFactory.get()
  var containerId: String = null

  start()

  def start(): Unit = {
    while (true) {
      try {
        val config = ContainerConfig.builder()
          .image("mysql").env("MYSQL_ROOT_PASSWORD=rootpass")
          .build()
        containerId = docker.createContainer(config).id
        docker.startContainer(containerId)
        return
      } catch {
        case e: ImageNotFoundException => retry(3)(docker.pull("mysql"))
      }
    }
  }

  private def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e if n > 1 =>
        retry(n - 1)(fn)
    }
  }

  lazy val ip = docker.inspectContainer(containerId).networkSettings.ipAddress

  def close(): Unit = {
    docker.killContainer(containerId)
    docker.removeContainer(containerId)
    DockerClientFactory.close(docker)
  }
}

class MySQLIntegrationSuite extends SparkFunSuite with BeforeAndAfterAll with SharedSQLContext {
  lazy val db: MySQLDatabase = new MySQLDatabase()
  var ip: String = null

  def url(ip: String): String = url(ip, "mysql")
  def url(ip: String, db: String): String = s"jdbc:mysql://$ip:3306/$db?user=root&password=rootpass"

  def waitForDatabase(ip: String, maxMillis: Long) {
    val before = System.currentTimeMillis()
    var lastException: java.sql.SQLException = null
    while (true) {
      if (System.currentTimeMillis() > before + maxMillis) {
        throw new java.sql.SQLException(s"Database not up after $maxMillis ms.", lastException)
      }
      try {
        val conn = java.sql.DriverManager.getConnection(url(ip))
        conn.close()
        return
      } catch {
        case e: java.sql.SQLException =>
          lastException = e
          java.lang.Thread.sleep(250)
      }
    }
  }

  def setupDatabase(ip: String) {
    val conn = java.sql.DriverManager.getConnection(url(ip))
    try {
      conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
      conn.prepareStatement("CREATE TABLE foo.tbl (x INTEGER, y TEXT(8))").executeUpdate()
      conn.prepareStatement("INSERT INTO foo.tbl VALUES (42,'fred')").executeUpdate()
      conn.prepareStatement("INSERT INTO foo.tbl VALUES (17,'dave')").executeUpdate()

      conn.prepareStatement("CREATE TABLE foo.numbers (onebit BIT(1), tenbits BIT(10), "
        + "small SMALLINT, med MEDIUMINT, nor INT, big BIGINT, deci DECIMAL(40,20), flt FLOAT, "
        + "dbl DOUBLE)").executeUpdate()
      conn.prepareStatement("INSERT INTO foo.numbers VALUES (b'0', b'1000100101', "
        + "17, 77777, 123456789, 123456789012345, 123456789012345.123456789012345, "
        + "42.75, 1.0000000000000002)").executeUpdate()

      conn.prepareStatement("CREATE TABLE foo.dates (d DATE, t TIME, dt DATETIME, ts TIMESTAMP, "
        + "yr YEAR)").executeUpdate()
      conn.prepareStatement("INSERT INTO foo.dates VALUES ('1991-11-09', '13:31:24', "
        + "'1996-01-01 01:23:45', '2009-02-13 23:31:30', '2001')").executeUpdate()

      // TODO: Test locale conversion for strings.
      conn.prepareStatement("CREATE TABLE foo.strings (a CHAR(10), b VARCHAR(10), c TINYTEXT, "
        + "d TEXT, e MEDIUMTEXT, f LONGTEXT, g BINARY(4), h VARBINARY(10), i BLOB)"
      ).executeUpdate()
      conn.prepareStatement("INSERT INTO foo.strings VALUES ('the', 'quick', 'brown', 'fox', " +
        "'jumps', 'over', 'the', 'lazy', 'dog')").executeUpdate()
    } finally {
      conn.close()
    }
  }

  override def beforeAll() {
    // If you load the MySQL driver here, DriverManager will deadlock.  The
    // MySQL driver gets loaded when its jar gets loaded, unlike the Postgres
    // and H2 drivers.
    // scalastyle:off classforname
    // Class.forName("com.mysql.jdbc.Driver")
    // scalastyle:on classforname
    super.beforeAll()
    waitForDatabase(db.ip, 60000)
    setupDatabase(db.ip)
    ip = db.ip
  }

  override def afterAll() {
    db.close()
  }

  test("Basic test") {
    val df = sqlContext.read.jdbc(url(ip, "foo"), "tbl", new Properties)
    val rows = df.collect()
    assert(rows.length == 2)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 2)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.String"))
  }

  test("Numeric types") {
    val df = sqlContext.read.jdbc(url(ip, "foo"), "numbers", new Properties)
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
    val df = sqlContext.read.jdbc(url(ip, "foo"), "dates", new Properties)
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
    val df = sqlContext.read.jdbc(url(ip, "foo"), "strings", new Properties)
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
    val df1 = sqlContext.read.jdbc(url(ip, "foo"), "numbers", new Properties)
    val df2 = sqlContext.read.jdbc(url(ip, "foo"), "dates", new Properties)
    val df3 = sqlContext.read.jdbc(url(ip, "foo"), "strings", new Properties)
    df1.write.jdbc(url(ip, "foo"), "numberscopy", new Properties)
    df2.write.jdbc(url(ip, "foo"), "datescopy", new Properties)
    df3.write.jdbc(url(ip, "foo"), "stringscopy", new Properties)
  }
}
