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
 * To run this test suite for a specific version (e.g., mysql:5.7.36):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:5.7.36
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLNamespaceSuite"
 * }}}
 */
@DockerTest
class MySQLNamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {
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

  val map = new CaseInsensitiveStringMap(
    Map("url" -> db.getJdbcUrl(dockerIp, externalPort),
      "driver" -> "com.mysql.jdbc.Driver").asJava)

  catalog.initialize("mysql", map)

  override def dataPreparation(conn: Connection): Unit = {}

  override def builtinNamespaces: Array[Array[String]] = Array()

  override val supportsSchemaComment: Boolean = false

  // Cannot get namespaces with conn.getMetaData.getSchemas
  // TODO testListNamespaces()
  // TODO testDropNamespaces()
}
