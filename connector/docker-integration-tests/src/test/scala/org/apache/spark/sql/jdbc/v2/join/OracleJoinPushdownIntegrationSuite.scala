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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{DockerJDBCIntegrationSuite, JdbcDialect, OracleDatabaseOnDocker, OracleDialect}
import org.apache.spark.sql.jdbc.v2.JDBCV2JoinPushdownIntegrationSuiteBase
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.tags.DockerTest

/**
 * The following are the steps to test this:
 *
 * 1. Choose to use a prebuilt image or build Oracle database in a container
 *    - The documentation on how to build Oracle RDBMS in a container is at
 *      https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
 *    - Official Oracle container images can be found at https://container-registry.oracle.com
 *    - Trustable and streamlined Oracle Database Free images can be found on Docker Hub at
 *      https://hub.docker.com/r/gvenzl/oracle-free
 *      see also https://github.com/gvenzl/oci-oracle-free
 * 2. Run: export ORACLE_DOCKER_IMAGE_NAME=image_you_want_to_use_for_testing
 *    - Example: export ORACLE_DOCKER_IMAGE_NAME=gvenzl/oracle-free:latest
 * 3. Run: export ENABLE_DOCKER_INTEGRATION_TESTS=1
 * 4. Start docker: sudo service docker start
 *    - Optionally, docker pull $ORACLE_DOCKER_IMAGE_NAME
 * 5. Run Spark integration tests for Oracle with: ./build/sbt -Pdocker-integration-tests
 *    "testOnly org.apache.spark.sql.jdbc.v2.OracleIntegrationSuite"
 *
 * A sequence of commands to build the Oracle Database Free container image:
 *  $ git clone https://github.com/oracle/docker-images.git
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles0
 *  $ ./buildContainerImage.sh -v 23.4.0 -f
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:23.4.0-free
 *
 * This procedure has been validated with Oracle Database Free version 23.4.0,
 * and with Oracle Express Edition versions 18.4.0 and 21.4.0
 */
@DockerTest
class OracleJoinPushdownIntegrationSuite
  extends DockerJDBCIntegrationSuite
  with JDBCV2JoinPushdownIntegrationSuiteBase {
  override def excluded: Seq[String] = Seq(
    // Following tests are harder to be supported for Oracle because Oracle connector does
    // casts in predicates. There is a separate test in this suite that is similar to
    // "Test explain formatted" test from base suite.
    "Test self join with condition",
    "Test multi-way self join with conditions",
    "Test explain formatted"
  )

  override val namespace: String = "SYSTEM"

  override val db = new OracleDatabaseOnDocker

  override val url = db.getJdbcUrl(dockerIp, externalPort)

  override val jdbcDialect: JdbcDialect = OracleDialect()

  override val integerType = DataTypes.createDecimalType(10, 0)

  override def caseConvert(identifier: String): String = identifier.toUpperCase(Locale.ROOT)

  override def schemaPreparation(): Unit = {}

  // This method comes from DockerJDBCIntegrationSuite
  override def dataPreparation(connection: Connection): Unit = {
    super.dataPreparation()
  }

  test("Test explain formatted - Oracle compatible") {
    val sqlQuery =
      s"""
         |SELECT * FROM $catalogAndNamespace.$casedJoinTableName1 a
         |JOIN $catalogAndNamespace.$casedJoinTableName2 b
         |ON a.id = b.id + 1
         |JOIN $catalogAndNamespace.$casedJoinTableName3 c
         |ON b.id = c.id + 1
         |JOIN $catalogAndNamespace.$casedJoinTableName4 d
         |ON c.id = d.id + 1
         |""".stripMargin

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      // scalastyle:off line.size.limit
      checkJoinPushed(
        df,
        s"""PushedFilters: [CAST(id_3 AS decimal(11,0)) = (id_4 + 1)], PushedJoins:\u0020
           |[L]: PushedFilters: [CAST(ID_1 AS decimal(11,0)) = (id_3 + 1)]
           |     PushedJoins:
           |     [L]: PushedFilters: [CAST(ID AS decimal(11,0)) = (ID_1 + 1)]
           |          PushedJoins:
           |          [L]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |               PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |          [R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName2)}
           |               PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |     [R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName3)}
           |          PushedFilters: [id IS NOT NULL]
           |[R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName4)}
           |     PushedFilters: [id IS NOT NULL]""".stripMargin
      )
      // scalastyle:on line.size.limit
    }
  }
}
