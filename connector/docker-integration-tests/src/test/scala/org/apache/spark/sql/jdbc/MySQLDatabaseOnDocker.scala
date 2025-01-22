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

class MySQLDatabaseOnDocker extends DatabaseOnDocker {
  override val imageName = sys.env.getOrElse("MYSQL_DOCKER_IMAGE_NAME", "mysql:9.1.0")
  override val env = Map(
    "MYSQL_ROOT_PASSWORD" -> "rootpass"
  )
  override val usesIpc = false
  override val jdbcPort: Int = 3306

  override def getJdbcUrl(ip: String, port: Int): String =
    s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass&allowPublicKeyRetrieval=true" +
      s"&useSSL=false&disableMariaDbDriver"
}
