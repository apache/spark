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

import org.apache.spark.sql.jdbc.{DB2DatabaseOnDocker, DockerJDBCIntegrationSuite}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., icr.io/db2_community/db2:11.5.9.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 DB2_DOCKER_IMAGE_NAME=icr.io/db2_community/db2:11.5.9.0
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.DB2NamespaceSuite"
 * }}}
 */
@DockerTest
class DB2NamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {
  override val db = new DB2DatabaseOnDocker
  val map = new CaseInsensitiveStringMap(
    Map("url" -> db.getJdbcUrl(dockerIp, externalPort),
      "driver" -> "com.ibm.db2.jcc.DB2Driver").asJava)

  catalog.initialize("db2", map)

  override def dataPreparation(conn: Connection): Unit = {}

  override def builtinNamespaces: Array[Array[String]] =
    Array(Array("NULLID"), Array("SQLJ"), Array("SYSCAT"), Array("SYSFUN"),
      Array("SYSIBM"), Array("SYSIBMADM"), Array("SYSIBMINTERNAL"), Array("SYSIBMTS"),
      Array("SYSPROC"), Array("SYSPUBLIC"), Array("SYSSTAT"), Array("SYSTOOLS"))

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    builtinNamespaces ++ Array(namespace)
  }

  override val supportsDropSchemaCascade: Boolean = false
}
