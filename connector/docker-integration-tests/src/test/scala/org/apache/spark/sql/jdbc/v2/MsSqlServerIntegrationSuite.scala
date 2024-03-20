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

import org.apache.spark.{SparkConf, SparkSQLFeatureNotSupportedException}
import org.apache.spark.sql.AnalysisException
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

  override def excluded: Seq[String] = Seq(
    "simple scan with OFFSET",
    "simple scan with LIMIT and OFFSET",
    "simple scan with paging: top N and OFFSET",
    "scan with aggregate push-down: VAR_POP with DISTINCT",
    "scan with aggregate push-down: COVAR_POP with DISTINCT",
    "scan with aggregate push-down: COVAR_POP without DISTINCT",
    "scan with aggregate push-down: COVAR_SAMP with DISTINCT",
    "scan with aggregate push-down: COVAR_SAMP without DISTINCT",
    "scan with aggregate push-down: CORR with DISTINCT",
    "scan with aggregate push-down: CORR without DISTINCT",
    "scan with aggregate push-down: REGR_INTERCEPT with DISTINCT",
    "scan with aggregate push-down: REGR_INTERCEPT without DISTINCT",
    "scan with aggregate push-down: REGR_SLOPE with DISTINCT",
    "scan with aggregate push-down: REGR_SLOPE without DISTINCT",
    "scan with aggregate push-down: REGR_R2 with DISTINCT",
    "scan with aggregate push-down: REGR_R2 without DISTINCT",
    "scan with aggregate push-down: REGR_SXY with DISTINCT",
    "scan with aggregate push-down: REGR_SXY without DISTINCT")

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
    var expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType()
      .add("ID", StringType, true, defaultMetadata())
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"STRING\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 55)
    )
  }

  override def testUpdateColumnNullability(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID STRING NOT NULL)")
    // Update nullability is unsupported for mssql db.
    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        sql(s"ALTER TABLE $tbl ALTER COLUMN ID DROP NOT NULL")
      },
      errorClass = "_LEGACY_ERROR_TEMP_2271")
  }
}
