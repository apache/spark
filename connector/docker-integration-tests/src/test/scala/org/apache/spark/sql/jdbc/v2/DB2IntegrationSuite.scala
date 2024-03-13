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
import java.util.Locale

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.DB2DatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., ibmcom/db2:11.5.8.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 DB2_DOCKER_IMAGE_NAME=ibmcom/db2:11.5.8.0
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.DB2IntegrationSuite"
 * }}}
 */
@DockerTest
class DB2IntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {

  override def excluded: Seq[String] = Seq(
    "scan with aggregate push-down: COVAR_POP with DISTINCT",
    "scan with aggregate push-down: COVAR_SAMP with DISTINCT",
    "scan with aggregate push-down: CORR with DISTINCT",
    "scan with aggregate push-down: CORR without DISTINCT",
    "scan with aggregate push-down: REGR_INTERCEPT with DISTINCT",
    "scan with aggregate push-down: REGR_SLOPE with DISTINCT",
    "scan with aggregate push-down: REGR_R2 with DISTINCT",
    "scan with aggregate push-down: REGR_SXY with DISTINCT")

  override val catalogName: String = "db2"
  override val namespaceOpt: Option[String] = Some("DB2INST1")
  override val db = new DB2DatabaseOnDocker
  override val connectionTimeout = timeout(3.minutes)

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.db2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.db2.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.db2.pushDownAggregate", "true")
    .set("spark.sql.catalog.db2.pushDownLimit", "true")
    .set("spark.sql.catalog.db2.pushDownOffset", "true")

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept INTEGER, name VARCHAR(10), salary DECIMAL(20, 2), bonus DOUBLE)")
      .executeUpdate()
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE DOUBLE")
    t = spark.table(tbl)
    expectedSchema = new StructType().add("ID", DoubleType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    // Update column type from DOUBLE to STRING
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE VARCHAR(10)"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"DOUBLE\"",
        "newType" -> "\"VARCHAR(10)\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 57)
    )
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('CCSID'='UNICODE')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
  }

  override def caseConvert(tableName: String): String = tableName.toUpperCase(Locale.ROOT)
}
