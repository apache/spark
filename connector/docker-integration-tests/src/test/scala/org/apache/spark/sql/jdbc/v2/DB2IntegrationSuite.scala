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
import org.apache.spark.sql.jdbc.DatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., ibmcom/db2:11.5.6.0a):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 DB2_DOCKER_IMAGE_NAME=ibmcom/db2:11.5.6.0a
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.DB2IntegrationSuite"
 * }}}
 */
@DockerTest
class DB2IntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {
  override val catalogName: String = "db2"
  override val namespaceOpt: Option[String] = Some("DB2INST1")
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("DB2_DOCKER_IMAGE_NAME", "ibmcom/db2:11.5.6.0a")
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

  override val connectionTimeout = timeout(3.minutes)

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.db2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.db2.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.db2.pushDownAggregate", "true")

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
    val msg1 = intercept[AnalysisException] {
      sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE VARCHAR(10)")
    }.getMessage
    assert(msg1.contains(
      s"Cannot update $catalogName.alt_table field ID: double cannot be cast to varchar"))
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('CCSID'='UNICODE')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
  }

  override def caseConvert(tableName: String): String = tableName.toUpperCase(Locale.ROOT)

  testVarPop()
  testVarPop(true)
  testVarSamp()
  testVarSamp(true)
  testStddevPop()
  testStddevPop(true)
  testStddevSamp()
  testStddevSamp(true)
  testCovarPop()
  testCovarSamp()
  testRegrIntercept()
  testRegrSlope()
  testRegrR2()
  testRegrSXY()
}
