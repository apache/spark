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

import org.apache.spark.sql.jdbc.{DockerJDBCIntegrationSuite, JdbcDialect, PostgresDatabaseOnDocker, PostgresDialect}
import org.apache.spark.sql.jdbc.v2.JDBCV2JoinPushdownIntegrationSuiteBase
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., postgres:17.2-alpine)
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:17.2-alpine
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.PostgresIntegrationSuite"
 * }}}
 */
@DockerTest
class PostgresJoinPushdownIntegrationSuite
  extends DockerJDBCIntegrationSuite
    with JDBCV2JoinPushdownIntegrationSuiteBase {
  override val db = new PostgresDatabaseOnDocker

  override val url = db.getJdbcUrl(dockerIp, externalPort)

  override val jdbcDialect: JdbcDialect = PostgresDialect()

  // This method comes from DockerJDBCIntegrationSuite
  override def dataPreparation(connection: Connection): Unit = {
    super.dataPreparation()
  }
}
