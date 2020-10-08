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

package org.apache.spark.sql.execution.datasources.jdbc.connection

import java.sql.Driver

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

private[jdbc] class MariaDBConnectionProvider extends SecureConnectionProvider {
  override val driverClass = "org.mariadb.jdbc.Driver"

  override val name: String = "mariadb"

  override def appEntry(driver: Driver, options: JDBCOptions): String =
    "Krb5ConnectorContext"

  override def setAuthenticationConfigIfNeeded(driver: Driver, options: JDBCOptions): Unit = {
    val (parent, configEntry) = getConfigWithAppEntry(driver, options)
    /**
     * Couple of things to mention here (v2.5.4 client):
     * 1. MariaDB doesn't support JAAS application name configuration
     * 2. MariaDB sets a default JAAS config if "java.security.auth.login.config" is not set
     */
    val entryUsesKeytab = configEntry != null &&
      configEntry.exists(_.getOptions().get("useKeyTab") == "true")
    if (configEntry == null || configEntry.isEmpty || !entryUsesKeytab) {
      setAuthenticationConfig(parent, driver, options)
    }
  }
}
