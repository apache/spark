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
import java.nio.file.Files

import com.github.dockerjava.api.model.{AccessMode, Bind, ContainerConfig, HostConfig, Volume}

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class OracleDatabaseOnDocker extends DatabaseOnDocker with Logging {
  // sarutak/oracle-free is a custom fork of gvenzl/oracle-free which allows to set timeout for
  // password initialization. See SPARK-54076 for details.
  lazy override val imageName =
    sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "sarutak/oracle-free:23.26.2-slim")
  val oracle_password = "Th1s1sThe0racle#Pass"
  override val env = Map(
    "ORACLE_PWD" -> oracle_password, // oracle images uses this
    "ORACLE_PASSWORD" -> oracle_password, // gvenzl/oracle-free uses this
    "PASSWORD_INIT_TIMEOUT" -> "60"
  )
  override val usesIpc = false
  override val jdbcPort: Int = 1521

  // The Oracle Free container is the heaviest of the JDBC integration test databases and
  // intermittently takes longer than the default 10 minutes to fully bootstrap its listener,
  // causing the suite to abort with "ORA-12541: No listener". Give it more headroom so the
  // test is not flaky on slow CI runners.
  override def connectionTimeout: Option[String] = Some("15min")

  // The listener refuses a dedicated connection with ORA-12516 once the processes the instance
  // last reported to it, plus the connections it has handed off since, reach PROCESSES. The
  // instance reports only every few seconds, the image's default PROCESSES of 200 leaves room
  // for about 110 connections on top of its background processes, and the JDBC v2 tests open
  // close to 100 short-lived connections within one such window. PROCESSES is a static
  // parameter, so a wrapper entrypoint raises it in the spfile before the image's own entrypoint
  // (container-entrypoint.sh of gvenzl/oracle-free and its forks) starts the database. Other
  // images, such as those built from oracle/docker-images, have no such entrypoint and start as
  // they are.
  private val entryPointDir = "/docker-entrypoint-oracle"
  private val entryPointScript = "oracle-docker-entrypoint.sh"
  private def wrapsEntryPoint: Boolean = imageName.contains("oracle-free")

  override def getEntryPoint: Option[String] =
    if (wrapsEntryPoint) Some(s"$entryPointDir/$entryPointScript") else None

  override def beforeContainerStart(
      hostConfigBuilder: HostConfig,
      containerConfigBuilder: ContainerConfig): Unit = if (wrapsEntryPoint) {
    val dir = Utils.createTempDir()
    val script = new File(dir, entryPointScript)
    Utils.tryWithResource(getClass.getClassLoader.getResourceAsStream(entryPointScript)) { in =>
      Files.copy(in, script.toPath)
    }
    // The database runs as the `oracle` user of the container, not as the owner of these files.
    Seq(dir, script).foreach { f =>
      f.setReadable(true, false)
      f.setExecutable(true, false)
    }
    val bind = new Bind(dir.getAbsolutePath, new Volume(entryPointDir), AccessMode.ro)
    hostConfigBuilder.withBinds(hostConfigBuilder.getBinds :+ bind: _*)
  }

  override def getJdbcUrl(ip: String, port: Int): String = {
    s"jdbc:oracle:thin:system/$oracle_password@//$ip:$port/freepdb1"
  }
}
