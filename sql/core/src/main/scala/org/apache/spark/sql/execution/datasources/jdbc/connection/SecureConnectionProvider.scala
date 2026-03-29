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
import javax.security.auth.login.{AppConfigurationEntry, Configuration}

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.SecurityUtils

private[jdbc] abstract class SecureConnectionProvider extends BasicConnectionProvider with Logging {
  /**
   * Returns the driver canonical class name which the connection provider supports.
   */
  protected val driverClass: String

  override def canHandle(driver: Driver, options: Map[String, String]): Boolean = {
    val jdbcOptions = new JDBCOptions(options)
    jdbcOptions.keytab != null && jdbcOptions.principal != null &&
      driverClass.equalsIgnoreCase(jdbcOptions.driverClass)
  }

  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    val jdbcOptions = new JDBCOptions(options)
    setAuthenticationConfig(driver, jdbcOptions)
    super.getConnection(driver: Driver, options: Map[String, String])
  }

  /**
   * Returns JAAS application name. This is sometimes configurable on the JDBC driver level.
   */
  def appEntry(driver: Driver, options: JDBCOptions): String

  private[connection] def setAuthenticationConfig(driver: Driver, options: JDBCOptions) = {
    val parent = Configuration.getConfiguration
    val config = new SecureConnectionProvider.JDBCConfiguration(
      parent, appEntry(driver, options), options.keytab,
      options.principal, options.refreshKrb5Config)
    logDebug("Adding database specific security configuration")
    Configuration.setConfiguration(config)
  }
}

object SecureConnectionProvider {
  class JDBCConfiguration(
    parent: Configuration,
    appEntry: String,
    keytab: String,
    principal: String,
    refreshKrb5Config: Boolean) extends Configuration {
  val entry =
    new AppConfigurationEntry(
      SecurityUtils.getKrb5LoginModuleName(),
      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
      Map[String, Object](
        "useTicketCache" -> "false",
        "useKeyTab" -> "true",
        "keyTab" -> keytab,
        "principal" -> principal,
        "debug" -> "true",
        "refreshKrb5Config" -> refreshKrb5Config.toString
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
}
