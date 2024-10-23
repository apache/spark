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

import org.apache.spark.{SparkConf, SparkRuntimeException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
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

  override def excluded: Seq[String] = Seq(
    "scan with aggregate push-down: VAR_POP with DISTINCT",
    "scan with aggregate push-down: VAR_SAMP with DISTINCT",
    "scan with aggregate push-down: STDDEV_POP with DISTINCT",
    "scan with aggregate push-down: STDDEV_SAMP with DISTINCT",
    "scan with aggregate push-down: COVAR_POP with DISTINCT",
    "scan with aggregate push-down: COVAR_SAMP with DISTINCT",
    "scan with aggregate push-down: CORR with DISTINCT",
    "scan with aggregate push-down: REGR_INTERCEPT with DISTINCT",
    "scan with aggregate push-down: REGR_SLOPE with DISTINCT",
    "scan with aggregate push-down: REGR_R2 with DISTINCT",
    "scan with aggregate push-down: REGR_SXY with DISTINCT")

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

  override def defaultMetadata(dataType: DataType): Metadata = new MetadataBuilder()
    .putLong("scale", 0)
    .putBoolean("isSigned", dataType.isInstanceOf[NumericType] || dataType.isInstanceOf[StringType])
    .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, "varchar(255)")
    .build()

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.oracle", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.oracle.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.oracle.pushDownAggregate", "true")
    .set("spark.sql.catalog.oracle.pushDownLimit", "true")
    .set("spark.sql.catalog.oracle.pushDownOffset", "true")

  override val connectionTimeout = timeout(7.minutes)

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept NUMBER(32), name VARCHAR2(32), salary NUMBER(20, 2)," +
        " bonus BINARY_DOUBLE)").executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE pattern_testing_table (
         |pattern_testing_col VARCHAR(50)
         |)
                   """.stripMargin
    ).executeUpdate()
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType()
      .add("ID", DecimalType(10, 0), true, super.defaultMetadata(DecimalType(10, 0)))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE LONG")
    t = spark.table(tbl)
    expectedSchema = new StructType()
      .add("ID", DecimalType(19, 0), true, super.defaultMetadata(DecimalType(19, 0)))
    assert(t.schema === expectedSchema)
    // Update column type from LONG to INTEGER
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"DECIMAL(19,0)\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 56)
    )
  }

  override def caseConvert(tableName: String): String = tableName.toUpperCase(Locale.ROOT)

  test("SPARK-46478: Revert SPARK-43049 to use varchar(255) for string") {
    val tableName = catalogName + ".t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(c1 string)")
      checkError(
        exception = intercept[SparkRuntimeException] {
          sql(s"INSERT INTO $tableName SELECT rpad('hi', 256, 'spark')")
        },
        errorClass = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "255")
      )
    }
  }
}
