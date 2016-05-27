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

import org.scalatest._

import org.apache.spark.tags.DockerTest

@DockerTest
@Ignore // AMPLab Jenkins needs to be updated before shared memory works on docker
class DB2IntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = "lresende/db2express-c:10.5.0.5-3.10.0"
    override val env = Map(
      "DB2INST1_PASSWORD" -> "rootpass",
      "LICENSE" -> "accept"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 50000
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:db2://$ip:$port/foo:user=db2inst1;password=rootpass;retrieveMessagesFromServerOnGetMessage=true;" //scalastyle:ignore
    override def getStartupProcessName: Option[String] = Some("db2start")
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE TABLE tbl (x INTEGER, y VARCHAR(8))").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement("CREATE TABLE numbers ( small SMALLINT, med INTEGER, big BIGINT, "
      + "deci DECIMAL(31,20), flt FLOAT, dbl DOUBLE)").executeUpdate()
    conn.prepareStatement("INSERT INTO numbers VALUES (17, 77777, 922337203685477580, "
      + "123456745.56789012345000000000, 42.75, 5.4E-70)").executeUpdate()

    conn.prepareStatement("CREATE TABLE dates (d DATE, t TIME, ts TIMESTAMP )").executeUpdate()
    conn.prepareStatement("INSERT INTO dates VALUES ('1991-11-09', '13:31:24', "
      + "'2009-02-13 23:31:30')").executeUpdate()

    // TODO: Test locale conversion for strings.
    conn.prepareStatement("CREATE TABLE strings (a CHAR(10), b VARCHAR(10), c CLOB, d BLOB)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO strings VALUES ('the', 'quick', 'brown', BLOB('fox'))")
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
    assert(types.length == 6)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.Integer"))
    assert(types(2).equals("class java.lang.Long"))
    assert(types(3).equals("class java.math.BigDecimal"))
    assert(types(4).equals("class java.lang.Double"))
    assert(types(5).equals("class java.lang.Double"))
    assert(rows(0).getInt(0) == 17)
    assert(rows(0).getInt(1) == 77777)
    assert(rows(0).getLong(2) == 922337203685477580L)
    val bd = new BigDecimal("123456745.56789012345000000000")
    assert(rows(0).getAs[BigDecimal](3).equals(bd))
    assert(rows(0).getDouble(4) == 42.75)
    assert(rows(0).getDouble(5) == 5.4E-70)
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
    assert(types.length == 4)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.String"))
    assert(types(2).equals("class java.lang.String"))
    assert(types(3).equals("class [B"))
    assert(rows(0).getString(0).equals("the       "))
    assert(rows(0).getString(1).equals("quick"))
    assert(rows(0).getString(2).equals("brown"))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](3), Array[Byte](102, 111, 120)))
  }

  test("Basic write test") {
    // val df1 = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties)
    val df2 = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
    val df3 = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    // df1.write.jdbc(jdbcUrl, "numberscopy", new Properties)
    df2.write.jdbc(jdbcUrl, "datescopy", new Properties)
    df3.write.jdbc(jdbcUrl, "stringscopy", new Properties)
  }
}
