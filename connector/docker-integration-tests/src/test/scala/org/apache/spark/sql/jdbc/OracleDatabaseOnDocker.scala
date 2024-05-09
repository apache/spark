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

import java.io.{File, PrintWriter}

import com.github.dockerjava.api.model._

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class OracleDatabaseOnDocker extends DatabaseOnDocker with Logging {
  lazy override val imageName =
    sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "gvenzl/oracle-free:23.4-slim")
  val oracle_password = "Th1s1sThe0racle#Pass"
  override val env = Map(
    "ORACLE_PWD" -> oracle_password, // oracle images uses this
    "ORACLE_PASSWORD" -> oracle_password // gvenzl/oracle-free uses this
  )
  override val usesIpc = false
  override val jdbcPort: Int = 1521

  override def getJdbcUrl(ip: String, port: Int): String = {
    s"jdbc:oracle:thin:system/$oracle_password@//$ip:$port/freepdb1"
  }

  override def beforeContainerStart(
      hostConfigBuilder: HostConfig,
      containerConfigBuilder: ContainerConfig): Unit = {
    try {
      val dir = Utils.createTempDir()
      val writer = new PrintWriter(new File(dir, "install.sql"))
      // SPARK-46592: gvenzl/oracle-free occasionally fails to start with the following error:
      // 'ORA-04021: timeout occurred while waiting to lock object', when initializing the
      // SYSTEM user. This is due to the fact that the default DDL_LOCK_TIMEOUT is 0, which
      // means that the lock will no wait. We set the timeout to 30 seconds to try again.
      // TODO: This workaround should be removed once the issue is fixed in the image.
      // https://github.com/gvenzl/oci-oracle-free/issues/35
      writer.write("ALTER SESSION SET DDL_LOCK_TIMEOUT = 30;\n")
      writer.write(s"""ALTER USER SYSTEM IDENTIFIED BY "$oracle_password";""")
      writer.close()
      val newBind = new Bind(
        dir.getAbsolutePath,
        new Volume("/docker-entrypoint-initdb.d"),
        AccessMode.DEFAULT)
      hostConfigBuilder.withBinds(hostConfigBuilder.getBinds :+ newBind: _*)
    } catch {
      case e: Exception =>
        logWarning("Failed to create install.sql file", e)
    }
  }
}
