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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ConnectionProviderSuite extends ConnectionProviderSuiteBase with SharedSparkSession {
  test("All built-in providers must be loaded") {
    IntentionallyFaultyConnectionProvider.constructed = false
    val providers = ConnectionProvider.loadProviders()
    assert(providers.exists(_.isInstanceOf[BasicConnectionProvider]))
    assert(providers.exists(_.isInstanceOf[DB2ConnectionProvider]))
    assert(providers.exists(_.isInstanceOf[MariaDBConnectionProvider]))
    assert(providers.exists(_.isInstanceOf[MSSQLConnectionProvider]))
    assert(providers.exists(_.isInstanceOf[PostgresConnectionProvider]))
    assert(providers.exists(_.isInstanceOf[OracleConnectionProvider]))
    assert(IntentionallyFaultyConnectionProvider.constructed)
    assert(!providers.exists(_.isInstanceOf[IntentionallyFaultyConnectionProvider]))
    assert(providers.size === 6)
  }

  test("Disabled provider must not be loaded") {
    withSQLConf(SQLConf.DISABLED_JDBC_CONN_PROVIDER_LIST.key -> "db2") {
      val providers = ConnectionProvider.loadProviders()
      assert(!providers.exists(_.isInstanceOf[DB2ConnectionProvider]))
      assert(providers.size === 5)
    }
  }

  test("Multiple security configs must be reachable") {
    Configuration.setConfiguration(null)
    val postgresProvider = new PostgresConnectionProvider()
    val postgresDriver = registerDriver(postgresProvider.driverClass)
    val postgresOptions = options("jdbc:postgresql://localhost/postgres")
    val postgresAppEntry = postgresProvider.appEntry(postgresDriver, postgresOptions)
    val db2Provider = new DB2ConnectionProvider()
    val db2Driver = registerDriver(db2Provider.driverClass)
    val db2Options = options("jdbc:db2://localhost/db2")
    val db2AppEntry = db2Provider.appEntry(db2Driver, db2Options)

    // Make sure no authentication for the databases are set
    val rootConfig = Configuration.getConfiguration
    assert(rootConfig.getAppConfigurationEntry(postgresAppEntry) == null)
    assert(rootConfig.getAppConfigurationEntry(db2AppEntry) == null)

    postgresProvider.setAuthenticationConfig(postgresDriver, postgresOptions)
    val postgresConfig = Configuration.getConfiguration

    db2Provider.setAuthenticationConfig(db2Driver, db2Options)
    val db2Config = Configuration.getConfiguration

    // Make sure authentication for the databases are set
    assert(rootConfig != postgresConfig)
    assert(rootConfig != db2Config)
    // The topmost config in the chain is linked with all the subsequent entries
    assert(db2Config.getAppConfigurationEntry(postgresAppEntry) != null)
    assert(db2Config.getAppConfigurationEntry(db2AppEntry) != null)

    Configuration.setConfiguration(null)
  }
}
