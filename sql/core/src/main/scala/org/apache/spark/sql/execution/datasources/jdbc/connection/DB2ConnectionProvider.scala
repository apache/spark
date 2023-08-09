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

private[sql] class DB2ConnectionProvider extends SecureConnectionProvider {
  override val driverClass = "com.ibm.db2.jcc.DB2Driver"

  override val name: String = "db2"

  override def appEntry(driver: Driver, options: JDBCOptions): String = "JaasClient"

  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    val jdbcOptions = new JDBCOptions(options)
    setAuthenticationConfig(driver, jdbcOptions)
    UserGroupInformation.loginUserFromKeytabAndReturnUGI(jdbcOptions.principal, jdbcOptions.keytab)
      .doAs(
        new PrivilegedExceptionAction[Connection]() {
          override def run(): Connection = {
            DB2ConnectionProvider.super.getConnection(driver, options)
          }
        }
      )
  }

  override def getAdditionalProperties(options: JDBCOptions): Properties = {
    val result = new Properties()
    // 11 is the integer value for kerberos
    result.put("securityMechanism", new String("11"))
    result.put("KerberosServerPrincipal", options.principal)
    result
  }
}
