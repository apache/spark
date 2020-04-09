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
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

private[jdbc] class MariaDBConnectionProvider(driver: Driver, options: JDBCOptions)
    extends SecureConnectionProvider(driver, options) {
  override val appEntry: String = {
    "Krb5ConnectorContext"
  }

  override def setAuthenticationConfigIfNeeded(): Unit = {
    val parent = Configuration.getConfiguration
    val configEntry = parent.getAppConfigurationEntry(appEntry)
    /**
     * Couple of things to mention here:
     * 1. MariaDB doesn't support JAAS application name configuration
     * 2. MariaDB sets a default JAAS config if "java.security.auth.login.config" is not set
     */
    val entryUsesKeytab = configEntry != null &&
      configEntry.exists(_.getOptions().get("useKeyTab") == "true")
    if (configEntry == null || configEntry.isEmpty || !entryUsesKeytab) {
      val config = new SecureConnectionProvider.JDBCConfiguration(
        parent, appEntry, options.keytab, options.principal)
      logDebug("Adding database specific security configuration")
      Configuration.setConfiguration(config)
    }
  }
}

private[sql] object MariaDBConnectionProvider {
  val driverClass = "org.mariadb.jdbc.Driver"
}
