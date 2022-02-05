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
 * The following are the steps to test this:
 *
 * 1. Choose to use a prebuilt image or build Oracle database in a container
 *    - The documentation on how to build Oracle RDBMS in a container is at
 *      https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
 *    - Official Oracle container images can be found at https://container-registry.oracle.com
 *    - A trustable and streamlined Oracle XE database image can be found on Docker Hub at
 *      https://hub.docker.com/r/gvenzl/oracle-xe see also https://github.com/gvenzl/oci-oracle-xe
 * 2. Run: export ORACLE_DOCKER_IMAGE_NAME=image_you_want_to_use_for_testing
 *    - Example: export ORACLE_DOCKER_IMAGE_NAME=gvenzl/oracle-xe:latest
 * 3. Run: export ENABLE_DOCKER_INTEGRATION_TESTS=1
 * 4. Start docker: sudo service docker start
 *    - Optionally, docker pull $ORACLE_DOCKER_IMAGE_NAME
 * 5. Run Spark integration tests for Oracle with: ./build/sbt -Pdocker-integration-tests
 *    "testOnly org.apache.spark.sql.jdbc.v2.OracleNamespaceSuite"
 *
 * A sequence of commands to build the Oracle XE database container image:
 *  $ git clone https://github.com/oracle/docker-images.git
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles
 *  $ ./buildContainerImage.sh -v 18.4.0 -x
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:18.4.0-xe
 *
 * This procedure has been validated with Oracle 18.4.0 Express Edition.
 */
@DockerTest
class OracleNamespaceSuite extends DockerJDBCIntegrationSuite with V2JDBCNamespaceTest {
  override val db = new DatabaseOnDocker {
    lazy override val imageName =
      sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "gvenzl/oracle-xe:18.4.0")
    val oracle_password = "Th1s1sThe0racle#Pass"
    override val env = Map(
      "ORACLE_PWD" -> oracle_password,      // oracle images uses this
      "ORACLE_PASSWORD" -> oracle_password  // gvenzl/oracle-xe uses this
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/$oracle_password@//$ip:$port/xe"
  }

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
