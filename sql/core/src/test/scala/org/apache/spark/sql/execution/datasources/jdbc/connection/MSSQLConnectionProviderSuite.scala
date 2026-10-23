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

import java.sql.{Connection, Driver, DriverPropertyInfo, SQLException}
import java.util.Properties
import java.util.logging.Logger

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class MSSQLConnectionProviderSuite extends ConnectionProviderSuiteBase {
  test("appEntry unwraps reflected parser exceptions") {
    val expected = new SQLException("parser failure")
    val driver = new ThrowingParserDriver(expected)

    val actual = intercept[SQLException] {
      new MSSQLConnectionProvider().appEntry(driver, options("jdbc:sqlserver://localhost/mssql"))
    }

    assert(actual eq expected)
  }

  test("setAuthenticationConfig default parser must set authentication all the time") {
    val provider = new MSSQLConnectionProvider()
    val driver = registerDriver(provider.driverClass)

    testProviders(driver, provider, options("jdbc:sqlserver://localhost/mssql"),
      options("jdbc:sqlserver://localhost/mssql;jaasConfigurationName=custommssql"))
  }

  test("setAuthenticationConfig custom parser must set authentication all the time") {
    val provider = new MSSQLConnectionProvider() {
      override val parserMethod: String = "IntentionallyNotExistingMethod"
    }
    val driver = registerDriver(provider.driverClass)

    testProviders(driver, provider, options("jdbc:sqlserver://localhost/mssql"),
      options("jdbc:sqlserver://localhost/mssql;jaasConfigurationName=custommssql"))
  }

  private def testProviders(
      driver: Driver,
      provider: SecureConnectionProvider,
      defaultOptions: JDBCOptions,
      customOptions: JDBCOptions) = {
    testSecureConnectionProvider(provider, driver, defaultOptions)
    testSecureConnectionProvider(provider, driver, customOptions)
  }
}

private class ThrowingParserDriver(failure: SQLException) extends Driver {
  def parseAndMergeProperties(url: String, properties: Properties): Properties = throw failure

  override def connect(url: String, info: Properties): Connection = null
  override def acceptsURL(url: String): Boolean = true
  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
    Array.empty
  override def getMajorVersion: Int = 0
  override def getMinorVersion: Int = 0
  override def jdbcCompliant(): Boolean = false
  override def getParentLogger: Logger = null
}
