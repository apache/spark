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

import org.apache.spark.sql.jdbc.{DockerJDBCIntegrationSuite, OracleDatabaseOnDocker}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
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
 *    "testOnly org.apache.spark.sql.jdbc.v2.OracleNamespaceSuite"
 *
 * A sequence of commands to build the Oracle Database Free container image:
 *  $ git clone https://github.com/oracle/docker-images.git
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles
 *  $ ./buildContainerImage.sh -v 23.4.0 -f
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:23.4.0-free
 *
 * This procedure has been validated with Oracle Database Free version 23.4.0,
 * and with Oracle Express Edition versions 18.4.0 and 21.4.0
 */
@DockerTest
class OracleNamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {

  override def excluded: Seq[String] = Seq("listNamespaces: basic behavior", "Drop namespace")

  override val db = new OracleDatabaseOnDocker

  val map = new CaseInsensitiveStringMap(
    Map("url" -> db.getJdbcUrl(dockerIp, externalPort),
      "driver" -> "oracle.jdbc.OracleDriver").asJava)

  catalog.initialize("system", map)

  override def dataPreparation(conn: Connection): Unit = {}

  override def builtinNamespaces: Array[Array[String]] =
    Array(Array("ANONYMOUS"), Array("APEX_030200"), Array("APEX_PUBLIC_USER"), Array("APPQOSSYS"),
      Array("BI"), Array("DIP"), Array("FLOWS_FILES"), Array("HR"), Array("OE"), Array("PM"),
      Array("SCOTT"), Array("SH"), Array("SPATIAL_CSW_ADMIN_USR"), Array("SPATIAL_WFS_ADMIN_USR"),
      Array("XS$NULL"))

  // Cannot create schema dynamically
  // TODO testListNamespaces()
  // TODO testDropNamespaces()
}
