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

class PostgresDatabaseOnDocker extends DatabaseOnDocker with Logging {
  lazy override val imageName: String =
    sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:17.2-alpine")
  private val postgres_user = "postgres"
  private val postgres_password = "rootpass"
  override val env: Map[String, String] = Map(
    "POSTGRES_PASSWORD" -> postgres_password
  )
  override val usesIpc = false
  override val jdbcPort: Int = 5432

  override def getJdbcUrl(ip: String, port: Int): String = {
    s"jdbc:postgresql://$ip:$port/postgres?user=$postgres_user&password=$postgres_password"
  }
}
