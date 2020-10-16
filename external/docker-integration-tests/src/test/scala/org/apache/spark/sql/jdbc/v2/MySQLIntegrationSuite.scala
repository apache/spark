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
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite}
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
class MySQLIntegrationSuite extends DockerJDBCIntegrationSuite with V2JDBCTest {
  override val catalogName: String = "mysql"
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

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER) USING _")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", IntegerType)
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType().add("ID", StringType)
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val msg1 = intercept[AnalysisException] {
      sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER")
    }.getMessage
    assert(msg1.contains("Cannot update alt_table field ID: string cannot be cast to int"))
  }

  override def testUpdateColumnNullability(tbl: String): Unit = {
    sql("CREATE TABLE mysql.alt_table (ID STRING NOT NULL) USING _")
    // Update nullability is unsupported for mysql db.
    val msg = intercept[AnalysisException] {
      sql("ALTER TABLE mysql.alt_table ALTER COLUMN ID DROP NOT NULL")
    }.getCause.asInstanceOf[SQLFeatureNotSupportedException].getMessage

    assert(msg.contains("UpdateColumnNullability is not supported"))
  }
}
