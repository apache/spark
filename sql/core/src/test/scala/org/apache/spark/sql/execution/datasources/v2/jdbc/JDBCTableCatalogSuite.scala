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
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
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
    // Check not existing namespace
    checkAnswer(sql("SHOW TABLES IN h2.bad_test"), Seq())
  }

  test("drop a table and test whether the table exists") {
    withConnection { conn =>
      conn.prepareStatement("""CREATE TABLE "test"."to_drop" (id INTEGER)""").executeUpdate()
    }
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "to_drop"), Row("test", "people")))
    sql("DROP TABLE h2.test.to_drop")
    checkAnswer(sql("SHOW TABLES IN h2.test"), Seq(Row("test", "people")))
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[NoSuchTableException] {
        sql(s"DROP TABLE $table")
      }
    }
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
    // Rename not existing table or namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[org.h2.jdbc.JdbcSQLException] {
        sql(s"ALTER TABLE $table RENAME TO test.dst_table")
      }
    }
    // Rename to an existing table
    withTable("h2.test.dst_table") {
      withConnection { conn =>
        conn.prepareStatement("""CREATE TABLE "test"."dst_table" (id INTEGER)""").executeUpdate()
      }
      withTable("h2.test.src_table") {
        withConnection { conn =>
          conn.prepareStatement("""CREATE TABLE "test"."src_table" (id INTEGER)""").executeUpdate()
        }
        intercept[org.h2.jdbc.JdbcSQLException] {
          sql("ALTER TABLE h2.test.src_table RENAME TO h2.test.dst_table")
        }
      }
    }
  }

  test("load a table") {
    val t = spark.table("h2.test.people")
    val expectedSchema = new StructType()
      .add("NAME", StringType)
      .add("ID", IntegerType)
    assert(t.schema === expectedSchema)
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        spark.table(s"h2.$table").schema
      }
    }
  }

  test("create a table") {
    withTable("h2.test.new_table") {
      // TODO (SPARK-32427): Omit USING in CREATE TABLE
      sql("CREATE TABLE h2.test.new_table(i INT, j STRING) USING _")
      checkAnswer(
        sql("SHOW TABLES IN h2.test"),
        Seq(Row("test", "people"), Row("test", "new_table")))
    }
    withTable("h2.test.new_table") {
      sql("CREATE TABLE h2.test.new_table(i INT, j STRING) USING _")
      intercept[AnalysisException] {
        sql("CREATE TABLE h2.test.new_table(i INT, j STRING) USING _")
      }
    }
    intercept[org.h2.jdbc.JdbcSQLException] {
      sql("CREATE TABLE h2.bad_test.new_table(i INT, j STRING) USING _")
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
      // Add already existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table ADD COLUMNS (C3 DOUBLE)")
      }
    }
    // Add a column to not existing table and namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ADD COLUMNS (C4 STRING)")
      }
    }
  }

  test("alter table ... rename column") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER, C0 INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table RENAME COLUMN ID TO C")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType()
        .add("C", IntegerType)
        .add("C0", IntegerType)
      assert(t.schema === expectedSchema)
      // Rename to already existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table RENAME COLUMN C TO C0")
      }
    }
    // Rename a column in not existing table and namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table RENAME COLUMN ID TO C")
      }
    }
  }

  test("alter table ... drop column") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (C1 INTEGER, C2 INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table DROP COLUMN C1")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("C2", IntegerType)
      assert(t.schema === expectedSchema)
      // Drop not existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table DROP COLUMN bad_column")
      }
    }
    // Drop a column to not existing table and namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table DROP COLUMN C1")
      }
    }
  }

  test("alter table ... update column type") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE h2.test.alt_table ALTER COLUMN id TYPE DOUBLE")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("ID", DoubleType)
      assert(t.schema === expectedSchema)
      // Update not existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table ALTER COLUMN bad_column TYPE DOUBLE")
      }
      // Update column to wrong type
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table ALTER COLUMN id TYPE bad_type")
      }
    }
    // Update column type in not existing table and namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ALTER COLUMN id TYPE DOUBLE")
      }
    }
  }

  test("alter table ... update column nullability") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER NOT NULL) USING _")
      sql("ALTER TABLE h2.test.alt_table ALTER COLUMN ID DROP NOT NULL")
      val t = spark.table("h2.test.alt_table")
      val expectedSchema = new StructType().add("ID", IntegerType, nullable = true)
      assert(t.schema === expectedSchema)
      // Update nullability of not existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table ALTER COLUMN bad_column DROP NOT NULL")
      }
    }
    // Update column nullability in not existing table and namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ALTER COLUMN ID DROP NOT NULL")
      }
    }
  }

  test("alter table ... update column comment not supported") {
    withTable("h2.test.alt_table") {
      sql("CREATE TABLE h2.test.alt_table (ID INTEGER) USING _")
      val thrown = intercept[java.sql.SQLFeatureNotSupportedException] {
        sql("ALTER TABLE h2.test.alt_table ALTER COLUMN ID COMMENT 'test'")
      }
      assert(thrown.getMessage.contains("Unsupported TableChange"))
      // Update comment for not existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE h2.test.alt_table ALTER COLUMN bad_column COMMENT 'test'")
      }
    }
    // Update column comments in not existing table and namespace
    Seq("h2.test.not_existing_table", "h2.bad_test.not_existing_table").foreach { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ALTER COLUMN ID COMMENT 'test'")
      }
    }
  }
}
