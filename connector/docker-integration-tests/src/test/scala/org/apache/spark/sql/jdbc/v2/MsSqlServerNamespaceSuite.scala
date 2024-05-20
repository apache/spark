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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.jdbc.{DockerJDBCIntegrationSuite, MsSQLServerDatabaseOnDocker}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., 2019-CU13-ubuntu-20.04):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1
 *   MSSQLSERVER_DOCKER_IMAGE_NAME=mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.MsSqlServerNamespaceSuite"
 * }}}
 */
@DockerTest
class MsSqlServerNamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {
  override val db = new MsSQLServerDatabaseOnDocker
  val map = new CaseInsensitiveStringMap(
    Map("url" -> db.getJdbcUrl(dockerIp, externalPort),
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver").asJava)

  catalog.initialize("mssql", map)

  override def dataPreparation(conn: Connection): Unit = {}

  override def builtinNamespaces: Array[Array[String]] =
    Array(Array("db_accessadmin"), Array("db_backupoperator"), Array("db_datareader"),
      Array("db_datawriter"), Array("db_ddladmin"), Array("db_denydatareader"),
      Array("db_denydatawriter"), Array("db_owner"), Array("db_securityadmin"), Array("dbo"),
      Array("guest"), Array("INFORMATION_SCHEMA"), Array("sys"))

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    builtinNamespaces ++ Array(namespace)
  }

  override val supportsSchemaComment: Boolean = false

  override val supportsDropSchemaCascade: Boolean = false
}
