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

import com.github.dockerjava.api.model.{AccessMode, Bind, ContainerConfig, HostConfig, Volume}

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class OracleDatabaseOnDocker extends DatabaseOnDocker with Logging {
  lazy override val imageName =
    sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "gvenzl/oracle-free:23.3")
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
      writer.write("ALTER SESSION SET DDL_LOCK_TIMEOUT = 10")
      writer.close()
      val newBind = new Bind(
        dir.getAbsolutePath,
        new Volume("/docker-entrypoint-initdb.d"),
        AccessMode.ro)
      hostConfigBuilder.withBinds(hostConfigBuilder.getBinds :+ newBind: _*)
    } catch {
      case e: Exception =>
        logWarning("Failed to create install.sql file", e)
    }
  }
}
