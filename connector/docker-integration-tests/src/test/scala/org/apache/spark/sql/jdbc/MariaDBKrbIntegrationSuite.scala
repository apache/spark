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

package org.apache.spark.sql.jdbc

import javax.security.auth.login.Configuration

import com.github.dockerjava.api.model.{AccessMode, Bind, ContainerConfig, HostConfig, Volume}

import org.apache.spark.sql.execution.datasources.jdbc.connection.SecureConnectionProvider
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mariadb:10.6.19):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MARIADB_DOCKER_IMAGE_NAME=mariadb:10.6.19
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly org.apache.spark.sql.jdbc.MariaDBKrbIntegrationSuite"
 * }}}
 */
@DockerTest
class MariaDBKrbIntegrationSuite extends DockerKrbJDBCIntegrationSuite {
  override protected val userName = s"mariadb/$dockerIp"
  override protected val keytabFileName = "mariadb.keytab"

  override val db = new MariaDBDatabaseOnDocker() {

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=$principal"

    override def beforeContainerStart(
        hostConfigBuilder: HostConfig,
        containerConfigBuilder: ContainerConfig): Unit = {
      copyExecutableResource("mariadb-docker-entrypoint.sh", entryPointDir, replaceIp)
      copyExecutableResource("mariadb-krb-setup.sh", initDbDir, replaceIp)

      val binds =
        Seq(entryPointDir -> "/docker-entrypoint", initDbDir -> "/docker-entrypoint-initdb.d")
          .map { case (from, to) =>
            new Bind(from.getAbsolutePath, new Volume(to), AccessMode.ro)
          }
      hostConfigBuilder.withBinds(hostConfigBuilder.getBinds ++ binds: _*)
    }
  }

  override protected def setAuthentication(keytabFile: String, principal: String): Unit = {
    val config = new SecureConnectionProvider.JDBCConfiguration(
      Configuration.getConfiguration, "Krb5ConnectorContext", keytabFile, principal, true)
    Configuration.setConfiguration(config)
  }
}
