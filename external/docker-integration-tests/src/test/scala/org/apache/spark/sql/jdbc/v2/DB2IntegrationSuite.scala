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

import java.sql.Connection

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
 * To run this test suite for a specific version (e.g., ibmcom/db2:11.5.4.0):
 * {{{
 *   DB2_DOCKER_IMAGE_NAME=ibmcom/db2:11.5.4.0
 *     ./build/sbt -Pdocker-integration-tests "test-only *DB2IntegrationSuite"
 * }}}
 */
@DockerTest
class DB2IntegrationSuite extends DockerJDBCIntegrationSuite with SharedSparkSession {
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

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.db2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.db2.url", db.getJdbcUrl(dockerIp, externalPort))

  override def dataPreparation(conn: Connection): Unit = {}

  test("SPARK-33034: ALTER TABLE ... update column type") {
    withTable("db2.alt_table") {
      sql("CREATE TABLE db2.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE db2.alt_table ALTER COLUMN id TYPE DOUBLE")
      val t = spark.table("db2.alt_table")
      val expectedSchema = new StructType().add("ID", DoubleType)
      assert(t.schema === expectedSchema)
      // Update column type from DOUBLE to STRING
      val msg1 = intercept[AnalysisException] {
        sql("ALTER TABLE db2.alt_table ALTER COLUMN id TYPE VARCHAR(10)")
      }.getMessage
      assert(msg1.contains("Cannot update alt_table field ID: double cannot be cast to varchar"))
      // Update not existing column
      val msg2 = intercept[AnalysisException] {
        sql("ALTER TABLE db2.alt_table ALTER COLUMN bad_column TYPE DOUBLE")
      }.getMessage
      assert(msg2.contains("Cannot update missing field bad_column"))
      // Update column to wrong type
      val msg3 = intercept[ParseException] {
        sql("ALTER TABLE db2.alt_table ALTER COLUMN id TYPE bad_type")
      }.getMessage
      assert(msg3.contains("DataType bad_type is not supported"))
    }
    // Update column type in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE db2.not_existing_table ALTER COLUMN id TYPE DOUBLE")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... update column nullability") {
    withTable("db2.alt_table") {
      sql("CREATE TABLE db2.alt_table (ID STRING NOT NULL) USING _")
      sql("ALTER TABLE db2.alt_table ALTER COLUMN ID DROP NOT NULL")
      val t = spark.table("db2.alt_table")
      val expectedSchema = new StructType().add("ID", StringType, nullable = true)
      assert(t.schema === expectedSchema)
      // Update nullability of not existing column
      val msg = intercept[AnalysisException] {
        sql("ALTER TABLE db2.alt_table ALTER COLUMN bad_column DROP NOT NULL")
      }.getMessage
      assert(msg.contains("Cannot update missing field bad_column"))
    }
    // Update column nullability in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE db2.not_existing_table ALTER COLUMN ID DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Table not found"))
  }
}
