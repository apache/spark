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

import test.scala.org.apache.spark.sql.jdbc.v2.V2JDBCPushdownTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite, MsSQLServerDatabaseOnDocker}

class MsSqlServerPushdownIntegrationSuite
  extends DockerJDBCIntegrationSuite
    with V2JDBCPushdownTest {

  override def excluded: Seq[String] = Seq(
    "case when in predicate and IIF push down")

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.sqlserver", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.sqlserver.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.sqlserver.pushDownAggregate", "true")
    .set("spark.sql.catalog.sqlserver.pushDownLimit", "true")

  override val db: DatabaseOnDocker = new MsSQLServerDatabaseOnDocker

  override protected val catalog: String = "sqlserver"
  override protected val tablePrefix: String = "testtbl"
  override protected val schema: String = "testschema"

  override protected def executeUpdate(sql: String): Unit = {
    getConnection().prepareStatement(sql).executeUpdate()
  }

  override def prepareTable(): Unit = {
    executeUpdate(
      s"""CREATE SCHEMA "$schema""""
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."$tablePrefix"
         | (id INTEGER, st VARCHAR(MAX), random_col INT);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_coalesce"
         | (id INTEGER, col1 VARCHAR(128), col2 INT);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_string_test"
         | (id INTEGER, st VARCHAR(MAX), random_col INT);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_with_nulls"
         | (id INTEGER, st VARCHAR(MAX));""".stripMargin
    )
  }

  /**
   * Prepare databases and tables for testing.
   */
  override def dataPreparation(connection: Connection): Unit = prepareData()

  override protected def commonAssertionOnDataFrame(df: DataFrame): Unit = {

  }
}
