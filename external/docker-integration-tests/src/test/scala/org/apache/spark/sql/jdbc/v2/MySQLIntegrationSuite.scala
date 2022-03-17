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
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.DatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:5.7.36):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:5.7.36
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {
  override val catalogName: String = "mysql"
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("MYSQL_DOCKER_IMAGE_NAME", "mysql:5.7.36")
    override val env = Map(
      "MYSQL_ROOT_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 3306

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/" +
        s"mysql?user=root&password=rootpass&allowPublicKeyRetrieval=true&useSSL=false"
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mysql.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.mysql.pushDownAggregate", "true")

  override val connectionTimeout = timeout(7.minutes)

  private var mySQLVersion = -1

  override def tablePreparation(connection: Connection): Unit = {
    mySQLVersion = connection.getMetaData.getDatabaseMajorVersion
    connection.prepareStatement(
      "CREATE TABLE employee (dept INT, name VARCHAR(32), salary DECIMAL(20, 2)," +
        " bonus DOUBLE)").executeUpdate()
  }

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
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $tbl ALTER COLUMN ID DROP NOT NULL")
    }.getCause.asInstanceOf[SQLFeatureNotSupportedException].getMessage

    assert(msg.contains("UpdateColumnNullability is not supported"))
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('ENGINE'='InnoDB', 'DEFAULT CHARACTER SET'='utf8')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
  }

  override def supportsIndex: Boolean = true

  override def indexOptions: String = "KEY_BLOCK_SIZE=10"

  testVarPop()
  testVarSamp()
  testStddevPop()
  testStddevSamp()
}
