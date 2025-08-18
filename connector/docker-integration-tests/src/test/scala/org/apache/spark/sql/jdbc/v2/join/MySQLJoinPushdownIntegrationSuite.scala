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

package org.apache.spark.sql.jdbc.v2.join

import java.sql.Connection
import java.util.Locale

import org.apache.spark.sql.jdbc.{DockerJDBCIntegrationSuite, JdbcDialect, MySQLDatabaseOnDocker, MySQLDialect}
import org.apache.spark.sql.jdbc.v2.JDBCV2JoinPushdownIntegrationSuiteBase
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:9.2.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:9.2.0
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MySQLIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLJoinPushdownIntegrationSuite
  extends DockerJDBCIntegrationSuite
    with JDBCV2JoinPushdownIntegrationSuiteBase {
  override val db = new MySQLDatabaseOnDocker

  override val url = db.getJdbcUrl(dockerIp, externalPort)

  override val jdbcDialect: JdbcDialect = MySQLDialect()

  override def caseConvert(identifier: String): String = identifier.toUpperCase(Locale.ROOT)

  override def remainColumnCase(identifier: String): String = "`" + identifier + "`"

  // This method comes from DockerJDBCIntegrationSuite
  override def dataPreparation(connection: Connection): Unit = {
    super.dataPreparation()
  }
}
