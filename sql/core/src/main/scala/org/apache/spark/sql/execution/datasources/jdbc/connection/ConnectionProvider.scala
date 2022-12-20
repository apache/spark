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
import java.util.ServiceLoader
import javax.security.auth.login.Configuration

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.security.SecurityConfigurationLock
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcConnectionProvider
import org.apache.spark.util.Utils

private[jdbc] object ConnectionProvider extends Logging {
  private val providers = loadProviders()

  def loadProviders(): Seq[JdbcConnectionProvider] = {
    val loader = ServiceLoader.load(classOf[JdbcConnectionProvider],
      Utils.getContextOrSparkClassLoader)
    val providers = mutable.ArrayBuffer[JdbcConnectionProvider]()

    val iterator = loader.iterator
    while (iterator.hasNext) {
      try {
        val provider = iterator.next
        logDebug(s"Loaded built-in provider: $provider")
        providers += provider
      } catch {
        case t: Throwable =>
          logError("Failed to load built-in provider.")
          logInfo("Loading of the provider failed with the exception:", t)
      }
    }

    val disabledProviders = Utils.stringToSeq(SQLConf.get.disabledJdbcConnectionProviders)
    // toSeq seems duplicate but it's needed for Scala 2.13
    providers.filterNot(p => disabledProviders.contains(p.name)).toSeq
  }

  def create(driver: Driver, options: Map[String, String]): Connection = {
    val filteredProviders = providers.filter(_.canHandle(driver, options))
    require(filteredProviders.size == 1,
      "JDBC connection initiated but not exactly one connection provider found which can handle " +
        s"it. Found active providers: ${filteredProviders.mkString(", ")}")
    val selectedProvider = filteredProviders.head
    if (selectedProvider.modifiesSecurityContext(driver, options)) {
      SecurityConfigurationLock.synchronized {
        // Inside getConnection it's safe to get parent again because SecurityConfigurationLock
        // makes sure it's untouched
        val parent = Configuration.getConfiguration
        try {
          selectedProvider.getConnection(driver, options)
        } finally {
          logDebug("Restoring original security configuration")
          Configuration.setConfiguration(parent)
        }
      }
    } else {
      selectedProvider.getConnection(driver, options)
    }
  }
}
