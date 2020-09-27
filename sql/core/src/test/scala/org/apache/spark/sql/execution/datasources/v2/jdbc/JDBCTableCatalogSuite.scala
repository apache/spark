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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCTableCatalogSuite extends QueryTest with SharedSparkSession {

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")

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
      conn.prepareStatement("""CREATE SCHEMA "test"""").executeUpdate()
      conn.prepareStatement(
        """CREATE TABLE "test"."people" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)""")
        .executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("show tables") {
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "people")))
  }

  test("drop a table and test whether the table exists") {
    withConnection { conn =>
      conn.prepareStatement("""CREATE TABLE "test"."to_drop" (id INTEGER)""").executeUpdate()
    }
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "to_drop"), Row("test", "people")))
    sql("DROP TABLE h2.test.to_drop")
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "people")))
  }

  test("rename a table") {
    withTable("h2.test.dst_table") {
      withConnection { conn =>
        conn.prepareStatement("""CREATE TABLE "test"."src_table" (id INTEGER)""").executeUpdate()
      }
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "src_table"), Row("test", "people")))
      sql("ALTER TABLE h2.test.src_table RENAME TO test.dst_table")
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "dst_table"), Row("test", "people")))
    }
  }

  test("load a table") {
    val t = spark.table("h2.test.people")
    val expectedSchema = new StructType()
      .add("NAME", StringType)
      .add("ID", IntegerType)
    assert(t.schema === expectedSchema)
  }

  test("create a table") {
    withTable("h2.test.new_table") {
      // TODO (SPARK-32427): Omit USING in CREATE TABLE
      sql("CREATE TABLE h2.test.new_table(i INT, j STRING) USING _")
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "people"), Row("test", "new_table")))
    }
  }

  test("alter table ... add column") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table ADD COLUMNS (C1 INTEGER, C2 STRING)")
      var t = spark.table("h2.test.alt_table")
      var expectedSchema = new StructType()
        .add("ID", IntegerType)
        .add("C1", IntegerType)
        .add("C2", StringType)
      assert(t.schema === expectedSchema)
      sql("ALTER TABLE h2.test.alt_table ADD COLUMNS (C3 DOUBLE)")
      t = spark.table("h2.test.alt_table")
      expectedSchema = expectedSchema.add("C3", DoubleType)
      assert(t.schema === expectedSchema)
    }
  }

  test("alter table ... rename column") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table RENAME COLUMN ID TO C")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("C", IntegerType)
      assert(t.schema === expectedSchema)
    }
  }

  test("alter table ... drop column") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (C1 INTEGER, C2 INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table DROP COLUMN C1")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("C2", IntegerType)
      assert(t.schema === expectedSchema)
    }
  }

  test("alter table ... update column type") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table ALTER COLUMN id TYPE DOUBLE")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("ID", DoubleType)
      assert(t.schema === expectedSchema)
    }
  }

  test("alter table ... update column nullability") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER NOT NULL) USING _")
      sql("ALTER TABLE h2.test.alt_table ALTER COLUMN ID DROP NOT NULL")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("ID", IntegerType, nullable = true)
      assert(t.schema === expectedSchema)
    }
  }

  test("alter table ... update column comment not supported") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER) USING _")
      val thrown = intercept[java.sql.SQLFeatureNotSupportedException] {
        sql("ALTER TABLE h2.test.alt_table ALTER COLUMN ID COMMENT 'test'")
      }
      assert(thrown.getMessage.contains("Unsupported TableChange"))
    }
  }
}
