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

import javax.security.auth.login.Configuration

class ConnectionProviderSuite extends ConnectionProviderSuiteBase {
  test("Multiple security configs must be reachable") {
    Configuration.setConfiguration(null)
    val postgresDriver = registerDriver(PostgresConnectionProvider.driverClass)
    val postgresProvider = new PostgresConnectionProvider(
      postgresDriver, options("jdbc:postgresql://localhost/postgres"))
    val db2Driver = registerDriver(DB2ConnectionProvider.driverClass)
    val db2Provider = new DB2ConnectionProvider(db2Driver, options("jdbc:db2://localhost/db2"))

    // Make sure no authentication for the databases are set
    val oldConfig = Configuration.getConfiguration
    assert(oldConfig.getAppConfigurationEntry(postgresProvider.appEntry) == null)
    assert(oldConfig.getAppConfigurationEntry(db2Provider.appEntry) == null)

    postgresProvider.setAuthenticationConfigIfNeeded()
    db2Provider.setAuthenticationConfigIfNeeded()

    // Make sure authentication for the databases are set
    val newConfig = Configuration.getConfiguration
    assert(oldConfig != newConfig)
    assert(newConfig.getAppConfigurationEntry(postgresProvider.appEntry) != null)
    assert(newConfig.getAppConfigurationEntry(db2Provider.appEntry) != null)
  }
}
