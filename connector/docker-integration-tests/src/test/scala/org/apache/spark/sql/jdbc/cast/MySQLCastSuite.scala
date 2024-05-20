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

package org.apache.spark.sql.jdbc.cast

import java.sql.{Connection, DriverManager}

import scala.collection.JavaConverters._

import com.databricks.sql.connector.JDBCConnectorCastSuiteBase

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.jdbc.MysqlTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite, JdbcDialect, MySQLDatabaseOnDocker, MySQLDialect}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

@DockerTest
class MySQLCastSuite extends DockerJDBCIntegrationSuite with JDBCConnectorCastSuiteBase {
  override val db: DatabaseOnDocker = new MySQLDatabaseOnDocker

  override def dataPreparation(connection: Connection): Unit = { }

  override protected def createConnection: Connection = {
    val jdbcUrl = db.getJdbcUrl("127.0.0.1", externalPort)
    DriverManager.getConnection(jdbcUrl, db.getJdbcProperties())
  }

  override def dialect: JdbcDialect = MySQLDialect

  override def tableCatalog: TableCatalog = {
    val catalog = new MysqlTableCatalog()
    val options = Map(
      "host" -> "127.0.0.1",
      "port" -> externalPort.toString,
      "database" -> "mysql",
      "user" -> "root",
      "password" -> "rootpass"
    )

    catalog.initialize("sfCat", new CaseInsensitiveStringMap(options.asJava))
    catalog
  }

  override protected def dropTable(table: Identifier): Unit =
    execUpdate(s"DROP TABLE ${table.toString}")

  override protected def dropSchema(schemaName: String): Unit = {
    // MySQL does not support CASCADE drop
    execUpdate(s"DROP SCHEMA $schemaName")
  }

  override def createNumericTypesTable: Identifier = {
    val identifier = Identifier.of(Array(schemaName), "CAST_NUMERIC_TABLE")
    execUpdate(
      s"""CREATE TABLE IF NOT EXISTS $schemaName.CAST_NUMERIC_TABLE
         |(COL_TINY TINYINT, COL_SMALL SMALLINT, COL_MEDIUM MEDIUMINT, COL_INT INT, COL_BIG BIGINT,
         | COL_DECIMAL DECIMAL(9,2), COL_FLOAT FLOAT, COL_DOUBLE DOUBLE)
         |""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_NUMERIC_TABLE VALUES
         |(-100, -100, -100, -100, -100,
         | -10.25, -10.256, -10.256)""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_NUMERIC_TABLE VALUES
         |(100, 100, 100, 100, 100,
         | 10.25, 10.256, 10.256)""".stripMargin)
    identifier
  }
}
