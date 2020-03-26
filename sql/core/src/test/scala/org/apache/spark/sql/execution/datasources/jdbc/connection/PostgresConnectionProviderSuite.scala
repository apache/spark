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

import java.sql.{Driver, DriverManager}
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCOptions}

class PostgresConnectionProviderSuite extends SparkFunSuite with BeforeAndAfterEach {
  private def options(url: String) = new JDBCOptions(Map[String, String](
    JDBCOptions.JDBC_URL -> url,
    JDBCOptions.JDBC_TABLE_NAME -> "table",
    JDBCOptions.JDBC_KEYTAB -> "/path/to/keytab",
    JDBCOptions.JDBC_PRINCIPAL -> "principal"
  ))

  override def afterEach(): Unit = {
    try {
      Configuration.setConfiguration(null)
    } finally {
      super.afterEach()
    }
  }

  test("setAuthenticationConfigIfNeeded must set authentication if not set") {
    DriverRegistry.register(PostgresConnectionProvider.driverClass)
    val driver = DriverManager.getDrivers.asScala.collectFirst {
      case d if d.getClass.getCanonicalName == PostgresConnectionProvider.driverClass => d
    }.get
    val defaultProvider = new PostgresConnectionProvider(
      driver, options("jdbc:postgresql://localhost/postgres"))
    val customProvider = new PostgresConnectionProvider(
      driver, options(s"jdbc:postgresql://localhost/postgres?jaasApplicationName=custompgjdbc"))

    assert(defaultProvider.appEntry !== customProvider.appEntry)

    // Make sure no authentication for postgres is set
    assert(Configuration.getConfiguration.getAppConfigurationEntry(
      defaultProvider.appEntry) == null)
    assert(Configuration.getConfiguration.getAppConfigurationEntry(
      customProvider.appEntry) == null)

    // Make sure the first call sets authentication properly
    val savedConfig = Configuration.getConfiguration
    defaultProvider.setAuthenticationConfigIfNeeded()
    val defaultConfig = Configuration.getConfiguration
    assert(savedConfig != defaultConfig)
    val defaultAppEntry = defaultConfig.getAppConfigurationEntry(defaultProvider.appEntry)
    assert(defaultAppEntry != null)
    customProvider.setAuthenticationConfigIfNeeded()
    val customConfig = Configuration.getConfiguration
    assert(savedConfig != customConfig)
    assert(defaultConfig != customConfig)
    val customAppEntry = customConfig.getAppConfigurationEntry(customProvider.appEntry)
    assert(customAppEntry != null)

    // Make sure a second call is not modifying the existing authentication
    defaultProvider.setAuthenticationConfigIfNeeded()
    customProvider.setAuthenticationConfigIfNeeded()
    assert(customConfig == Configuration.getConfiguration)
    assert(defaultConfig.getAppConfigurationEntry(defaultProvider.appEntry) === defaultAppEntry)
    assert(customConfig.getAppConfigurationEntry(customProvider.appEntry) === customAppEntry)
  }
}
