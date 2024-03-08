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

import java.sql.{Connection, Driver}
import javax.security.auth.login.Configuration

import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.security.SecurityConfigurationLock
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.jdbc.JdbcConnectionProvider
import org.apache.spark.sql.test.SharedSparkSession

class ConnectionProviderSuite
  extends ConnectionProviderSuiteBase
  with SharedSparkSession
  with MockitoSugar {

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

  test("Throw an error selecting from an empty list of providers on create") {
    val providerBase = new ConnectionProviderBase() {
      override val providers = Seq.empty
    }

    val err1 = intercept[IllegalArgumentException] {
      providerBase.create(mock[Driver], Map.empty, None)
    }
    assert(err1.getMessage.contains("Empty list of JDBC connection providers"))

    val err2 = intercept[IllegalArgumentException] {
      providerBase.create(mock[Driver], Map.empty, Some("test"))
    }
    assert(err2.getMessage.contains("Empty list of JDBC connection providers"))
  }

  test("Throw an error when more than one provider is available on create") {
    val provider1 = new JdbcConnectionProvider() {
      override val name: String = "test1"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection =
        throw new RuntimeException()
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = false
    }
    val provider2 = new JdbcConnectionProvider() {
      override val name: String = "test2"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection =
        throw new RuntimeException()
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = false
    }

    val providerBase = new ConnectionProviderBase() {
      override val providers = Seq(provider1, provider2)
    }

    val err = intercept[IllegalArgumentException] {
      providerBase.create(mock[Driver], Map.empty, None)
    }
    assert(err.getMessage.contains("more than one connection provider was found"))
  }

  test("Handle user specified JDBC connection provider") {
    val provider1 = new JdbcConnectionProvider() {
      override val name: String = "test1"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection =
        throw new RuntimeException()
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = false
    }
    val provider2 = new JdbcConnectionProvider() {
      override val name: String = "test2"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection =
        mock[Connection]
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = false
    }

    val providerBase = new ConnectionProviderBase() {
      override val providers = Seq(provider1, provider2)
    }
    // We don't expect any exceptions or null here
    assert(providerBase.create(mock[Driver], Map.empty, Some("test2")).isInstanceOf[Connection])
  }

  test("Synchronize on SecurityConfigurationLock when the specified connection provider needs") {
    val provider1 = new JdbcConnectionProvider() {
      override val name: String = "test1"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
        assert(Thread.holdsLock(SecurityConfigurationLock))
        mock[Connection]
      }
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = true
    }
    val provider2 = new JdbcConnectionProvider() {
      override val name: String = "test2"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
        assert(!Thread.holdsLock(SecurityConfigurationLock))
        mock[Connection]
      }
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = false
    }

    val providerBase = new ConnectionProviderBase() {
      override val providers = Seq(provider1, provider2)
    }
    // We don't expect any exceptions or null here
    assert(providerBase.create(mock[Driver], Map.empty, Some("test1")).isInstanceOf[Connection])
    assert(providerBase.create(mock[Driver], Map.empty, Some("test2")).isInstanceOf[Connection])
  }

  test("Throw an error when user specified provider that does not exist") {
    val provider = new JdbcConnectionProvider() {
      override val name: String = "provider"
      override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
      override def getConnection(driver: Driver, options: Map[String, String]): Connection =
        throw new RuntimeException()
      override def modifiesSecurityContext(
        driver: Driver,
        options: Map[String, String]
      ): Boolean = false
    }

    val providerBase = new ConnectionProviderBase() {
      override val providers = Seq(provider)
    }
    val err = intercept[IllegalArgumentException] {
      providerBase.create(mock[Driver], Map.empty, Some("test"))
    }
    assert(err.getMessage.contains("Could not find a JDBC connection provider with name 'test'"))
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

class DisallowedConnectionProviderSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      StaticSQLConf.DISABLED_JDBC_CONN_PROVIDER_LIST.key, "db2")

  test("Disabled provider must not be loaded") {
    val providers = ConnectionProvider.loadProviders()
    assert(!providers.exists(_.isInstanceOf[DB2ConnectionProvider]))
    assert(providers.size === 5)
  }
}
