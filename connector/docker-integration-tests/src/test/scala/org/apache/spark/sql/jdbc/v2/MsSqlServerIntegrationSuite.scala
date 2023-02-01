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
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.DatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., 2019-CU13-ubuntu-20.04):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1
 *   MSSQLSERVER_DOCKER_IMAGE_NAME=mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MsSqlServerIntegrationSuite"
 * }}}
 */
@DockerTest
class MsSqlServerIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {

  override val catalogName: String = "mssql"

  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("MSSQLSERVER_DOCKER_IMAGE_NAME",
      "mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04")
    override val env = Map(
      "SA_PASSWORD" -> "Sapass123",
      "ACCEPT_EULA" -> "Y"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1433

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:sqlserver://$ip:$port;user=sa;password=Sapass123;"
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mssql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mssql.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.mssql.pushDownAggregate", "true")
    .set("spark.sql.catalog.mssql.pushDownLimit", "true")

  override val connectionTimeout = timeout(7.minutes)

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept INT, name VARCHAR(32), salary NUMERIC(20, 2), bonus FLOAT)")
      .executeUpdate()
  }

  override def notSupportsTableComment: Boolean = true

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val msg1 = intercept[AnalysisException] {
      sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER")
    }.getMessage
    assert(msg1.contains(
      s"Cannot update $catalogName.alt_table field ID: string cannot be cast to int"))
  }

  override def testUpdateColumnNullability(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID STRING NOT NULL)")
    // Update nullability is unsupported for mssql db.
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $tbl ALTER COLUMN ID DROP NOT NULL")
    }.getCause.asInstanceOf[SQLFeatureNotSupportedException].getMessage

    assert(msg.contains("UpdateColumnNullability is not supported"))
  }

  test("simple scan with LIMIT") {
    val df = sql(s"SELECT * FROM $catalogAndNamespace." +
      s"${caseConvert("employee")} WHERE dept > 0 LIMIT 1")
    assert(limitPushed(df, 1))
    val row = df.collect()
    assert(row.length === 1)
    assert(row(0).getInt(0) === 1)
    assert(row(0).getString(1) === "amy")
    assert(row(0).getDecimal(2) === new java.math.BigDecimal("10000.00"))
    assert(row(0).getDouble(3) === 1000d)
  }

  private def checkSortRemoved(df: DataFrame): Unit = {
    val sorts = df.queryExecution.optimizedPlan.collect {
      case s: Sort => s
    }
    assert(sorts.isEmpty)
  }

  test("simple scan with top N") {
    val df = sql(s"SELECT * FROM $catalogAndNamespace." +
      s"${caseConvert("employee")} WHERE dept > 0 ORDER BY salary LIMIT 1")
    assert(limitPushed(df, 1))
    checkSortRemoved(df)
    val row = df.collect()
    assert(row.length === 1)
    assert(row(0).getInt(0) === 1)
    assert(row(0).getString(1) === "cathy")
    assert(row(0).getDecimal(2) === new java.math.BigDecimal("9000.00"))
    assert(row(0).getDouble(3) === 1200d)
  }

  testVarPop()
  testVarPop(true)
  testVarSamp()
  testVarSamp(true)
  testStddevPop()
  testStddevPop(true)
  testStddevSamp()
  testStddevSamp(true)
}
