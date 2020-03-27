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
import java.util.Properties
import javax.security.auth.login.{AppConfigurationEntry, Configuration}

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.connection.PostgresConnectionProvider.PGJDBCConfiguration
import org.apache.spark.util.SecurityUtils

private[jdbc] class PostgresConnectionProvider(driver: Driver, options: JDBCOptions)
    extends BasicConnectionProvider(driver, options) {
  val appEntry: String = {
    val parseURL = driver.getClass.getMethod("parseURL", classOf[String], classOf[Properties])
    val properties = parseURL.invoke(driver, options.url, null).asInstanceOf[Properties]
    properties.getProperty("jaasApplicationName", "pgjdbc")
  }

  def setAuthenticationConfigIfNeeded(): Unit = {
    val parent = Configuration.getConfiguration
    val configEntry = parent.getAppConfigurationEntry(appEntry)
    if (configEntry == null || configEntry.isEmpty) {
      val config = new PGJDBCConfiguration(parent, appEntry, options.keytab, options.principal)
      Configuration.setConfiguration(config)
    }
  }

  override def getConnection(): Connection = {
    setAuthenticationConfigIfNeeded()
    super.getConnection()
  }
}

private[sql] object PostgresConnectionProvider {
  class PGJDBCConfiguration(
      parent: Configuration,
      appEntry: String,
      keytab: String,
      principal: String) extends Configuration {
    private val entry =
      new AppConfigurationEntry(
        SecurityUtils.getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        Map[String, Object](
          "useTicketCache" -> "false",
          "useKeyTab" -> "true",
          "keyTab" -> keytab,
          "principal" -> principal,
          "debug" -> "true"
        ).asJava
      )

    override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
      if (name.equals(appEntry)) {
        Array(entry)
      } else {
        parent.getAppConfigurationEntry(name)
      }
    }
  }

  val driverClass = "org.postgresql.Driver"
}
