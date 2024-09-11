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

import org.apache.spark.{SparkConf, SparkSQLFeatureNotSupportedException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.MySQLDatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:9.0.1):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:9.0.1
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {

  override def excluded: Seq[String] = Seq(
    "scan with aggregate push-down: VAR_POP with DISTINCT",
    "scan with aggregate push-down: VAR_SAMP with DISTINCT",
    "scan with aggregate push-down: STDDEV_POP with DISTINCT",
    "scan with aggregate push-down: STDDEV_SAMP with DISTINCT",
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

  override val catalogName: String = "mysql"
  override val db = new MySQLDatabaseOnDocker

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mysql.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.mysql.pushDownAggregate", "true")
    .set("spark.sql.catalog.mysql.pushDownLimit", "true")
    .set("spark.sql.catalog.mysql.pushDownOffset", "true")

  private var mySQLVersion = -1

  override def tablePreparation(connection: Connection): Unit = {
    mySQLVersion = connection.getMetaData.getDatabaseMajorVersion
    connection.prepareStatement(
      "CREATE TABLE employee (dept INT, name VARCHAR(32), salary DECIMAL(20, 2)," +
        " bonus DOUBLE)").executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE pattern_testing_table (
         |pattern_testing_col LONGTEXT
         |)
                   """.stripMargin
    ).executeUpdate()
  }

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
      condition = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"STRING\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 55)
    )
  }

  override def testRenameColumn(tbl: String): Unit = {
    assert(mySQLVersion > 0)
    if (mySQLVersion < 8) {
      // Rename is unsupported for mysql versions < 8.0.
      val exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tbl RENAME COLUMN ID TO RENAMED")
      }
      assert(exception.getCause != null, s"Wrong exception thrown: $exception")
      val msg = exception.getCause.asInstanceOf[SQLFeatureNotSupportedException].getMessage
      assert(msg.contains("Rename column is only supported for MySQL version 8.0 and above."))
    } else {
      super.testRenameColumn(tbl)
    }
  }

  override def testUpdateColumnNullability(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID STRING NOT NULL)")
    // Update nullability is unsupported for mysql db.
    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        sql(s"ALTER TABLE $tbl ALTER COLUMN ID DROP NOT NULL")
      },
      condition = "_LEGACY_ERROR_TEMP_2271")
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('ENGINE'='InnoDB', 'DEFAULT CHARACTER SET'='utf8')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
  }

  override def supportsIndex: Boolean = true

  override def supportListIndexes: Boolean = true

  override def indexOptions: String = "KEY_BLOCK_SIZE=10"

  test("SPARK-42943: Use LONGTEXT instead of TEXT for StringType for effective length") {
    val tableName = catalogName + ".t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(c1 string)")
      sql(s"INSERT INTO $tableName SELECT rpad('hi', 65536, 'spark')")
      assert(sql(s"SELECT char_length(c1) from $tableName").head().get(0) === 65536)
    }
  }
}

/**
 * To run this test suite for a specific version (e.g., mysql:9.0.1):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:9.0.1
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly *MySQLOverMariaConnectorIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLOverMariaConnectorIntegrationSuite extends MySQLIntegrationSuite {
  override def defaultMetadata(dataType: DataType = StringType): Metadata = new MetadataBuilder()
    .putLong("scale", 0)
    .putBoolean("isTimestampNTZ", false)
    .putBoolean("isSigned", true)
    .build()

  override val db = new MySQLDatabaseOnDocker {
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass&allowPublicKeyRetrieval=true" +
        s"&useSSL=false"
  }
}
