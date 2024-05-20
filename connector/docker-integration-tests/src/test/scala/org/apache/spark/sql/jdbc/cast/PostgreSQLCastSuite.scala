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
import org.apache.spark.sql.execution.datasources.v2.jdbc.PostgresqlTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite, JdbcDialect, PostgresDialect}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

@DockerTest
class PostgreSQLCastSuite extends DockerJDBCIntegrationSuite with JDBCConnectorCastSuiteBase {
  override val schemaName: String = "cast_schema"

  override val db: DatabaseOnDocker = new DatabaseOnDocker {
    override val imageName: String =
      sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:16.2-alpine")
    override val env: Map[String, String] = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
  }

  override def dataPreparation(connection: Connection): Unit = { }

  override protected def createConnection: Connection = {
    val jdbcUrl = db.getJdbcUrl("127.0.0.1", externalPort)
    DriverManager.getConnection(jdbcUrl, db.getJdbcProperties())
  }

  override def dialect: JdbcDialect = PostgresDialect

  override def tableCatalog: TableCatalog = {
    val catalog = new PostgresqlTableCatalog()
    val options = Map(
      "host" -> "127.0.0.1",
      "port" -> externalPort.toString,
      "database" -> "postgres",
      "user" -> "postgres",
      "password" -> "rootpass"
    )

    catalog.initialize("postgresCat", new CaseInsensitiveStringMap(options.asJava))
    catalog
  }

  override protected def dropTable(table: Identifier): Unit =
    execUpdate(s"DROP TABLE ${table.toString}")

  override def createNumericTypesTable: Identifier = {
    val identifier = Identifier.of(Array(schemaName), "cast_numeric_table")
    execUpdate(
      s"""CREATE TABLE IF NOT EXISTS $schemaName.cast_numeric_table
         |(COL_SMALLINT SMALLINT, COL_INT INT, COL_BIGINT BIGINT,
         | COL_DECIMAL DECIMAL(9,2), COL_REAL REAL, COL_DOUBLE FLOAT8)
         |""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.cast_numeric_table VALUES
         |(-1000, -1000, -1000,
         | -10.25, -10.256, -10.256)""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.cast_numeric_table VALUES
         |(1000, 1000, 1000,
         | 10.25, 10.256, 10.256)""".stripMargin)
    identifier
  }

  override def createStringTypeTable: Identifier = {
    super.createStringTypeTable
    // Return lower case table name
    Identifier.of(Array(schemaName), "cast_string_table")
  }
}
