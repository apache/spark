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

package org.apache.spark.sql.jdbc.v2

import java.sql.{Connection, SQLFeatureNotSupportedException}

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 *
 * To run this test suite for a specific version (e.g., mysql:5.7.31):
 * {{{
 * MYSQL_DOCKER_IMAGE_NAME=mysql:5.7.31
 *         ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLIntegrationSuite"
 *
 * }}}
 *
 */
@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSparkSession {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("MYSQL_DOCKER_IMAGE_NAME", "mysql:5.7.31")
    override val env = Map(
      "MYSQL_ROOT_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 3306
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass"
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mysql.url", db.getJdbcUrl(dockerIp, externalPort))

  override val connectionTimeout = timeout(7.minutes)
  override def dataPreparation(conn: Connection): Unit = {}

  test("ALTER TABLE - add new columns") {
    withTable("mysql.alt_table") {
      sql("CREATE TABLE mysql.alt_table (ID STRING) USING _")
      sql("ALTER TABLE mysql.alt_table ADD COLUMNS (C1 STRING, C2 STRING)")
      var t = spark.table("mysql.alt_table")
      var expectedSchema = new StructType()
        .add("ID", StringType)
        .add("C1", StringType)
        .add("C2", StringType)
      assert(t.schema === expectedSchema)
      sql("ALTER TABLE mysql.alt_table ADD COLUMNS (C3 STRING)")
      t = spark.table("mysql.alt_table")
      expectedSchema = expectedSchema.add("C3", StringType)
      assert(t.schema === expectedSchema)
      // Add already existing column
      val msg = intercept[AnalysisException] {
        sql(s"ALTER TABLE mysql.alt_table ADD COLUMNS (C3 DOUBLE)")
      }.getMessage
      assert(msg.contains("Cannot add column, because C3 already exists"))
    }
    // Add a column to not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE mysql.not_existing_table ADD COLUMNS (C4 STRING)")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("ALTER TABLE - update column type") {
    withTable("mysql.alt_table") {
      sql("CREATE TABLE mysql.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE mysql.alt_table ALTER COLUMN id TYPE STRING")
      val t = spark.table("mysql.alt_table")
      val expectedSchema = new StructType().add("ID", StringType)
      assert(t.schema === expectedSchema)
      // Update column type from STRING to INTEGER
      val msg1 = intercept[AnalysisException] {
        sql("ALTER TABLE mysql.alt_table ALTER COLUMN id TYPE INTEGER")
      }.getMessage
      assert(msg1.contains("Cannot update alt_table field ID: string cannot be cast to int"))
      // Update not existing column
      val msg2 = intercept[AnalysisException] {
        sql("ALTER TABLE mysql.alt_table ALTER COLUMN bad_column TYPE DOUBLE")
      }.getMessage
      assert(msg2.contains("Cannot update missing field bad_column"))
      // Update column to wrong type
      val msg3 = intercept[ParseException] {
        sql("ALTER TABLE mysql.alt_table ALTER COLUMN id TYPE bad_type")
      }.getMessage
      assert(msg3.contains("DataType bad_type is not supported"))
    }
    // Update column type in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE mysql.not_existing_table ALTER COLUMN id TYPE DOUBLE")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("ALTER TABLE - update column nullability") {
    withTable("mysql.alt_table") {
      sql("CREATE TABLE mysql.alt_table (ID STRING NOT NULL) USING _")
      // Update nullability is unsupported for mysql db.
      val msg = intercept[AnalysisException] {
        sql("ALTER TABLE mysql.alt_table ALTER COLUMN ID DROP NOT NULL")
      }.getCause.asInstanceOf[SQLFeatureNotSupportedException].getMessage

      assert(msg.contains("UpdateColumnNullability is not supported"))
    }
  }
}
