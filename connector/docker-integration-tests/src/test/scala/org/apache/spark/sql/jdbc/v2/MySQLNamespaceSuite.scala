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

import org.apache.spark.SparkSQLFeatureNotSupportedException
import org.apache.spark.sql.connector.catalog.NamespaceChange
import org.apache.spark.sql.jdbc.{DockerJDBCIntegrationSuite, MySQLDatabaseOnDocker}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:9.0.1):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:9.0.1
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLNamespaceSuite"
 * }}}
 */
@DockerTest
class MySQLNamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {
  override val db = new MySQLDatabaseOnDocker

  val map = new CaseInsensitiveStringMap(
    Map("url" -> db.getJdbcUrl(dockerIp, externalPort),
      "driver" -> "com.mysql.cj.jdbc.Driver").asJava)

  catalog.initialize("mysql", map)

  override def dataPreparation(conn: Connection): Unit = {}

  override def builtinNamespaces: Array[Array[String]] =
    Array(Array("information_schema"), Array("mysql"), Array("performance_schema"), Array("sys"))

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    Array(builtinNamespaces.head, namespace) ++ builtinNamespaces.tail
  }

  override val supportsSchemaComment: Boolean = false

  override val supportsDropSchemaRestrict: Boolean = false

  test("Create or remove comment of namespace unsupported") {
    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        catalog.createNamespace(Array("foo"), Map("comment" -> "test comment").asJava)
      },
      errorClass = "UNSUPPORTED_FEATURE.COMMENT_NAMESPACE",
      parameters = Map("namespace" -> "`foo`")
    )
    assert(catalog.namespaceExists(Array("foo")) === false)
    catalog.createNamespace(Array("foo"), Map.empty[String, String].asJava)
    assert(catalog.namespaceExists(Array("foo")) === true)
    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        catalog.alterNamespace(
          Array("foo"),
          NamespaceChange.setProperty("comment", "comment for foo"))
      },
      errorClass = "UNSUPPORTED_FEATURE.COMMENT_NAMESPACE",
      parameters = Map("namespace" -> "`foo`")
    )

    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        catalog.alterNamespace(Array("foo"), NamespaceChange.removeProperty("comment"))
      },
      errorClass = "UNSUPPORTED_FEATURE.REMOVE_NAMESPACE_COMMENT",
      parameters = Map("namespace" -> "`foo`")
    )

    checkError(
      exception = intercept[SparkSQLFeatureNotSupportedException] {
        catalog.dropNamespace(Array("foo"), cascade = false)
      },
      errorClass = "UNSUPPORTED_FEATURE.DROP_NAMESPACE",
      parameters = Map("namespace" -> "`foo`")
    )
    catalog.dropNamespace(Array("foo"), cascade = true)
    assert(catalog.namespaceExists(Array("foo")) === false)
  }
}
