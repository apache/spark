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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.SecurityUtils

private[jdbc] abstract class SecureConnectionProvider(driver: Driver, options: JDBCOptions)
    extends BasicConnectionProvider(driver, options) with Logging {
  override def getConnection(): Connection = {
    setAuthenticationConfigIfNeeded()
    super.getConnection()
  }

  /**
   * Returns JAAS application name. This is sometimes configurable on the JDBC driver level.
   */
  val appEntry: String

  /**
   * Sets database specific authentication configuration when needed. If configuration already set
   * then later calls must be no op.
   */
  def setAuthenticationConfigIfNeeded(): Unit

  protected def getConfigWithAppEntry(): (Configuration, Array[AppConfigurationEntry]) = {
    val parent = Configuration.getConfiguration
    (parent, parent.getAppConfigurationEntry(appEntry))
  }

  protected def setAuthenticationConfig(parent: Configuration) = {
    val config = new SecureConnectionProvider.JDBCConfiguration(
      parent, appEntry, options.keytab, options.principal)
    logDebug("Adding database specific security configuration")
    Configuration.setConfiguration(config)
  }
}

object SecureConnectionProvider {
  class JDBCConfiguration(
    parent: Configuration,
    appEntry: String,
    keytab: String,
    principal: String) extends Configuration {
  val entry =
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
}
