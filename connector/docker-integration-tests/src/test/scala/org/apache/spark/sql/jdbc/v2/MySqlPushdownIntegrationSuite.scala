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
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite, MySQLDatabaseOnDocker}

class MySqlPushdownIntegrationSuite
  extends DockerJDBCIntegrationSuite
    with V2JDBCPushdownTest {

  override val db: DatabaseOnDocker = new MySQLDatabaseOnDocker

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mysql.url", db.getJdbcUrl(dockerIp, externalPort))
    .set("spark.sql.catalog.mysql.pushDownAggregate", "true")
    .set("spark.sql.catalog.mysql.pushDownLimit", "true")

  /**
   * Prepare databases and tables for testing.
   */
  override def dataPreparation(connection: Connection): Unit = prepareData()

  override protected val catalog: String = "mysql"
  override protected val tablePrefix: String = "testtbl"
  override protected val schema: String = "testschema"

  override protected def executeUpdate(sql: String): Unit = {
    getConnection().prepareStatement(sql).executeUpdate()
  }

  override protected def commonAssertionOnDataFrame(df: DataFrame): Unit = {

  }
}
