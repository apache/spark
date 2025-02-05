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

import org.apache.spark.internal.Logging

class OracleDatabaseOnDocker extends DatabaseOnDocker with Logging {
  lazy override val imageName =
    sys.env.getOrElse("ORACLE_DOCKER_IMAGE_NAME", "gvenzl/oracle-free:23.6-slim")
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
}
