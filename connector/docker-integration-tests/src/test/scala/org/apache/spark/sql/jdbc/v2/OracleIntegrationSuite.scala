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
 * The following are the steps to test this:
 *
 * 1. Choose to use a prebuilt image or build Oracle database in a container
 *    - The documentation on how to build Oracle RDBMS in a container is at
 *      https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
 *    - Official Oracle container images can be found at https://container-registry.oracle.com
 *    - A trustable and streamlined Oracle XE database image can be found on Docker Hub at
 *      https://hub.docker.com/r/gvenzl/oracle-xe see also https://github.com/gvenzl/oci-oracle-xe
 * 2. Run: export ORACLE_DOCKER_IMAGE_NAME=image_you_want_to_use_for_testing
 *    - Example: export ORACLE_DOCKER_IMAGE_NAME=gvenzl/oracle-xe:latest
 * 3. Run: export ENABLE_DOCKER_INTEGRATION_TESTS=1
 * 4. Start docker: sudo service docker start
 *    - Optionally, docker pull $ORACLE_DOCKER_IMAGE_NAME
 * 5. Run Spark integration tests for Oracle with: ./build/sbt -Pdocker-integration-tests
 *    "testOnly org.apache.spark.sql.jdbc.v2.OracleIntegrationSuite"
 *
 * A sequence of commands to build the Oracle XE database container image:
 *  $ git clone https://github.com/oracle/docker-images.git
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles
 *  $ ./buildContainerImage.sh -v 21.3.0 -x
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:21.3.0-xe
 *
 * This procedure has been validated with Oracle 18.4.0 and 21.3.0 Express Edition.
 */
@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {
  override val catalogName: String = "oracle"
  override val namespaceOpt: Option[String] = Some("SYSTEM")
  override val db = new DatabaseOnDocker {
    lazy override val imageName =
      sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "gvenzl/oracle-xe:21.3.0")
    val oracle_password = "Th1s1sThe0racle#Pass"
    override val env = Map(
      "ORACLE_PWD" -> oracle_password,      // oracle images uses this
      "ORACLE_PASSWORD" -> oracle_password  // gvenzl/oracle-xe uses this
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/$oracle_password@//$ip:$port/xe"
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.oracle", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.oracle.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.oracle.pushDownAggregate", "true")

  override val connectionTimeout = timeout(7.minutes)

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept NUMBER(32), name VARCHAR2(32), salary NUMBER(20, 2)," +
        " bonus BINARY_DOUBLE)").executeUpdate()
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", DecimalType(10, 0), true, defaultMetadata)
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

  override def caseConvert(tableName: String): String = tableName.toUpperCase(Locale.ROOT)

  testVarPop()
  testVarSamp()
  testStddevPop()
  testStddevSamp()
  testCovarPop()
  testCovarSamp()
  testCorr()
  testRegrIntercept()
  testRegrSlope()
  testRegrR2()
  testRegrSXY()
}
