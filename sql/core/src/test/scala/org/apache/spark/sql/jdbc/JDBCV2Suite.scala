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

import java.sql.{Connection, DriverManager}
import java.util.{Locale, Properties}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCV2Suite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.jdbc", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.jdbc.url", url)
    .set("spark.sql.catalog.jdbc.driver", "org.h2.Driver")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("CREATE SCHEMA test").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE test.people (name TEXT(32) NOT NULL, id INTEGER NOT NULL)").executeUpdate()
      conn.prepareStatement("INSERT INTO test.people VALUES ('fred', 1)").executeUpdate()
      conn.prepareStatement("INSERT INTO test.people VALUES ('mary', 2)").executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  private def listTables(): Seq[String] = {
    withConnection { conn =>
      val resultSet = conn.getMetaData.getTables(null, "TEST", "%", Array("TABLE"))
      val tables = ArrayBuffer.empty[String]
      while (resultSet.next()) {
        val tblName = resultSet.getString(3)
        tables.append(tblName)
      }
      tables.map(_.toLowerCase(Locale.ROOT))
    }
  }

  test("simple scan") {
    checkAnswer(sql("SELECT name, id FROM jdbc.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
  }

  test("create/drop table") {
    // TODO: currently CREATE TABLE without USING will be treated as Hive style CREATE TABLE, which
    //       is unexpected.
    sql("CREATE TABLE jdbc.test.abc(i INT, j STRING) USING x")
    assert(listTables().contains("abc"))
    sql("DROP TABLE jdbc.test.abc")
    assert(!listTables().contains("abc"))
  }

  test("create table as select") {
    withTable("jdbc.test.abc") {
      sql("CREATE TABLE jdbc.test.abc USING x AS SELECT * FROM jdbc.test.people")
      checkAnswer(sql("SELECT name, id FROM jdbc.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("insert") {
    withTable("jdbc.test.abc") {
      sql("CREATE TABLE jdbc.test.abc USING x AS SELECT * FROM jdbc.test.people")
      sql("INSERT INTO jdbc.test.abc SELECT 'lucy', 3")
      checkAnswer(
        sql("SELECT name, id FROM jdbc.test.abc"),
        Seq(Row("fred", 1), Row("mary", 2), Row("lucy", 3)))
      sql("INSERT OVERWRITE jdbc.test.abc SELECT 'bob', 4")
      checkAnswer(sql("SELECT name, id FROM jdbc.test.abc"), Row("bob", 4))
    }
  }
}
