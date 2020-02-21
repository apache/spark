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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class PostgresConnectionProviderSuite extends SparkFunSuite with BeforeAndAfterEach {
  private val options = new JDBCOptions(Map[String, String](
    JDBCOptions.JDBC_URL -> "jdbc:postgresql://localhost/postgres",
    JDBCOptions.JDBC_TABLE_NAME -> "table",
    JDBCOptions.JDBC_KEYTAB -> "keytab",
    JDBCOptions.JDBC_PRINCIPAL -> "principal"
  ))

  var provider: PostgresConnectionProvider = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    provider = new PostgresConnectionProvider(null, options)
  }

  override def afterEach(): Unit = {
    try {
      Configuration.setConfiguration(null)
    } finally {
      super.afterEach()
    }
  }

  test("setAuthenticationConfigIfNeeded must set authentication if not set") {
    // Make sure no authentication for postgres is set
    assert(Configuration.getConfiguration.getAppConfigurationEntry(
      PostgresConnectionProvider.appEntry) == null)

    // Make sure the first call sets authentication properly
    val savedConfig = Configuration.getConfiguration
    provider.setAuthenticationConfigIfNeeded()
    val postgresConfig = Configuration.getConfiguration
    assert(savedConfig != postgresConfig)
    val postgresAppEntry = postgresConfig.getAppConfigurationEntry(
      PostgresConnectionProvider.appEntry)
    assert(postgresAppEntry != null)

    // Make sure a second call is not modifying the existing authentication
    provider.setAuthenticationConfigIfNeeded()
    assert(postgresConfig == Configuration.getConfiguration)
    assert(postgresConfig.getAppConfigurationEntry(PostgresConnectionProvider.appEntry) ===
      postgresAppEntry)
  }
}
