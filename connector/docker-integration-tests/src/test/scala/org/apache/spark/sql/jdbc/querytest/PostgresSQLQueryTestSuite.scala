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

import java.io.File
import java.sql.Connection

import org.apache.spark.tags.DockerTest

/**
 * READ THIS IF YOU ADDED A NEW SQL TEST AND THIS SUITE IS FAILING:
 * Your new SQL test is automatically opted into this suite. It is likely failing because it is not
 * compatible with the default Postgres. You have two options:
 * 1. (Recommended) Modify your queries to be compatible with both systems. This is recommended
 *    because it will run your queries against postgres, providing higher correctness testing
 *    confidence, and you won't have to manually verify the golden files generated with your test.
 * 2. Add this line to your .sql file: --ONLY_IF spark
 *
 * Note: To run this test suite for a specific version (e.g., postgres:17.2-alpine):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:17.2-alpine
 *     ./build/sbt -Pdocker-integration-tests
 *     "testOnly org.apache.spark.sql.jdbc.PostgreSQLQueryTestSuite"
 * }}}
 */
@DockerTest
class PostgresSQLQueryTestSuite extends CrossDbmsQueryTestSuite {

  val DATABASE_NAME = CrossDbmsQueryTestSuite.POSTGRES
  // Scope to only subquery directory for now.
  protected val customInputFilePath: String = new File(inputFilePath, "subquery").getAbsolutePath

  override val db = new PostgresDatabaseOnDocker

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement(
      // Custom function `double` to imitate Spark's function, so that more tests are covered.
      """
        |CREATE OR REPLACE FUNCTION double(numeric_value numeric) RETURNS double precision
        |    AS 'select CAST($1 AS double precision);'
        |    LANGUAGE SQL
        |    IMMUTABLE
        |    RETURNS NULL ON NULL INPUT;
        |""".stripMargin
    ).executeUpdate()
  }

  listTestCases.foreach(createScalaTestCase)
}
