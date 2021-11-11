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

import java.security.PrivilegedExceptionAction
import java.sql.{Connection, Driver}
import java.util.Properties

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

private[sql] class MSSQLConnectionProvider extends SecureConnectionProvider {
  override val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val parserMethod: String = "parseAndMergeProperties"

  override val name: String = "mssql"

  override def appEntry(driver: Driver, options: JDBCOptions): String = {
    val configName = "jaasConfigurationName"
    val appEntryDefault = "SQLJDBCDriver"

    val parseURL = try {
      // The default parser method signature is the following:
      // private Properties parseAndMergeProperties(String Url, Properties suppliedProperties)
      val m = driver.getClass.getDeclaredMethod(parserMethod, classOf[String], classOf[Properties])
      m.setAccessible(true)
      Some(m)
    } catch {
      case _: NoSuchMethodException => None
    }

    parseURL match {
      case Some(m) =>
        logDebug("Property parser method found, using it")
        m.invoke(driver, options.url, null).asInstanceOf[Properties]
          .getProperty(configName, appEntryDefault)

      case None =>
        logDebug("Property parser method not found, using custom parsing mechanism")
        options.url.split(';').map(_.split('='))
          .find(kv => kv.length == 2 && kv(0) == configName)
          .getOrElse(Array(configName, appEntryDefault))(1)
    }
  }

  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    val jdbcOptions = new JDBCOptions(options)
    setAuthenticationConfig(driver, jdbcOptions)
    UserGroupInformation.loginUserFromKeytabAndReturnUGI(jdbcOptions.principal, jdbcOptions.keytab)
      .doAs(
        new PrivilegedExceptionAction[Connection]() {
          override def run(): Connection = {
            MSSQLConnectionProvider.super.getConnection(driver, options)
          }
        }
      )
  }

  override def getAdditionalProperties(options: JDBCOptions): Properties = {
    val result = new Properties()
    // These props needed to reach internal kerberos authentication in the JDBC driver
    result.put("integratedSecurity", "true")
    result.put("authenticationScheme", "JavaKerberos")
    result
  }
}
