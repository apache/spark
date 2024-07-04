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

import org.apache.spark.sql.jdbc.MariaDBDatabaseOnDocker
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StringType}
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mariadb:10.5.25):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MARIADB_DOCKER_IMAGE_NAME=mariadb:10.5.25
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly *v2*MariaDBIntegrationSuite"
 * }}}
 */
@DockerTest
class MariaDBIntegrationSuite extends MySQLIntegrationSuite {

  override def defaultMetadata(dataType: DataType = StringType): Metadata = new MetadataBuilder()
    .putLong("scale", 0)
    .putBoolean("isTimestampNTZ", false)
    .putBoolean("isSigned", true)
    .build()

  override val db = new MariaDBDatabaseOnDocker
}
