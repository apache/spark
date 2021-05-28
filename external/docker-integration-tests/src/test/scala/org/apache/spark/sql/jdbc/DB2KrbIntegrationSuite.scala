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

import java.security.PrivilegedExceptionAction
import java.sql.Connection
import javax.security.auth.login.Configuration

import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.connection.{DB2ConnectionProvider, SecureConnectionProvider}
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., ibmcom/db2:11.5.4.0):
 * {{{
 *   DB2_DOCKER_IMAGE_NAME=ibmcom/db2:11.5.4.0
 *     ./build/sbt -Pdocker-integration-tests "testOnly *DB2KrbIntegrationSuite"
 * }}}
 */
@DockerTest
class DB2KrbIntegrationSuite extends DockerKrbJDBCIntegrationSuite {
  override protected val userName = s"db2/$dockerIp"
  override protected val keytabFileName = "db2.keytab"

  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("DB2_DOCKER_IMAGE_NAME", "ibmcom/db2:11.5.4.0")
    override val env = Map(
      "DB2INST1_PASSWORD" -> "rootpass",
      "LICENSE" -> "accept",
      "DBNAME" -> "db2",
      "ARCHIVE_LOGS" -> "false",
      "AUTOCONFIG" -> "false"
    )
    override val usesIpc = false
    override val jdbcPort = 50000
    override val privileged = true
    override def getJdbcUrl(ip: String, port: Int): String = s"jdbc:db2://$ip:$port/db2"
    override def getJdbcProperties() = {
      val options = new JDBCOptions(Map[String, String](
        JDBCOptions.JDBC_URL -> getJdbcUrl(dockerIp, externalPort),
        JDBCOptions.JDBC_TABLE_NAME -> "bar",
        JDBCOptions.JDBC_KEYTAB -> keytabFileName,
        JDBCOptions.JDBC_PRINCIPAL -> principal
      ))
      new DB2ConnectionProvider().getAdditionalProperties(options)
    }

    override def beforeContainerStart(
        hostConfigBuilder: HostConfig.Builder,
        containerConfigBuilder: ContainerConfig.Builder): Unit = {
      copyExecutableResource("db2_krb_setup.sh", initDbDir, replaceIp)

      hostConfigBuilder.appendBinds(
        HostConfig.Bind.from(initDbDir.getAbsolutePath)
          .to("/var/custom").readOnly(true).build()
      )
    }
  }

  override val connectionTimeout = timeout(3.minutes)

  override protected def setAuthentication(keytabFile: String, principal: String): Unit = {
    val config = new SecureConnectionProvider.JDBCConfiguration(
      Configuration.getConfiguration, "JaasClient", keytabFile, principal, true)
    Configuration.setConfiguration(config)
  }

  override def getConnection(): Connection = {
    val config = new org.apache.hadoop.conf.Configuration
    SecurityUtil.setAuthenticationMethod(KERBEROS, config)
    UserGroupInformation.setConfiguration(config)

    UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFullPath).doAs(
      new PrivilegedExceptionAction[Connection]() {
        override def run(): Connection = {
          DB2KrbIntegrationSuite.super.getConnection()
        }
      }
    )
  }
}
