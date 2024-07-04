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
 * To run this test suite for a specific version (e.g., postgres:16.3-alpine):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:16.3-alpine
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly *PostgresKrbIntegrationSuite"
 * }}}
 */
@DockerTest
class PostgresKrbIntegrationSuite extends DockerKrbJDBCIntegrationSuite {
  override protected val userName = s"postgres/$dockerIp"
  override protected val keytabFileName = "postgres.keytab"

  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:16.3-alpine")
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=$principal&gsslib=gssapi"

    override def beforeContainerStart(
        hostConfigBuilder: HostConfig,
        containerConfigBuilder: ContainerConfig): Unit = {
      copyExecutableResource("postgres_krb_setup.sh", initDbDir, replaceIp)
      val newBind = new Bind(
        initDbDir.getAbsolutePath,
        new Volume("/docker-entrypoint-initdb.d"),
        AccessMode.ro)
      hostConfigBuilder.withBinds(hostConfigBuilder.getBinds :+ newBind: _*)
    }
  }

  override protected def setAuthentication(keytabFile: String, principal: String): Unit = {
    val config = new SecureConnectionProvider.JDBCConfiguration(
      Configuration.getConfiguration, "pgjdbc", keytabFile, principal, true)
    Configuration.setConfiguration(config)
  }
}
