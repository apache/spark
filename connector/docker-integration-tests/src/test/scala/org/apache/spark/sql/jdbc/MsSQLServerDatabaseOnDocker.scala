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

class MsSQLServerDatabaseOnDocker extends DatabaseOnDocker {
  override val imageName = sys.env.getOrElse("MSSQLSERVER_DOCKER_IMAGE_NAME",
    "mcr.microsoft.com/mssql/server:2022-CU15-ubuntu-22.04")
  override val env = Map(
    "SA_PASSWORD" -> "Sapass123",
    "ACCEPT_EULA" -> "Y"
  )
  override val usesIpc = false
  override val jdbcPort: Int = 1433

  override def getJdbcUrl(ip: String, port: Int): String =
    s"jdbc:sqlserver://$ip:$port;user=sa;password=Sapass123;" +
      "encrypt=true;trustServerCertificate=true"
}
