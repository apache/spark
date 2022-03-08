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

import scala.collection.JavaConverters._

import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., postgres:14.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:14.0
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.PostgresNamespaceSuite"
 * }}}
 */
@DockerTest
class PostgresNamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:14.0-alpine")
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
  }

  val map = new CaseInsensitiveStringMap(
    Map("url" -> db.getJdbcUrl(dockerIp, externalPort),
      "driver" -> "org.postgresql.Driver").asJava)

  catalog.initialize("postgresql", map)

  override def dataPreparation(conn: Connection): Unit = {}

  override def builtinNamespaces: Array[Array[String]] =
    Array(Array("information_schema"), Array("pg_catalog"), Array("public"))

  testListNamespaces()
  testDropNamespaces()
}
