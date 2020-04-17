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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
 * Connection provider which opens connection toward various databases (database specific instance
 * needed). If kerberos authentication required then it's the provider's responsibility to set all
 * the parameters.
 */
private[jdbc] trait ConnectionProvider {
  /**
   * Opens connection toward the database.
   */
  def getConnection(): Connection
}

private[jdbc] object ConnectionProvider extends Logging {
  def create(driver: Driver, options: JDBCOptions): ConnectionProvider = {
    if (options.keytab == null || options.principal == null) {
      logDebug("No authentication configuration found, using basic connection provider")
      new BasicConnectionProvider(driver, options)
    } else {
      logDebug("Authentication configuration found, using database specific connection provider")
      options.driverClass match {
        case PostgresConnectionProvider.driverClass =>
          logDebug("Postgres connection provider found")
          new PostgresConnectionProvider(driver, options)

        case MariaDBConnectionProvider.driverClass =>
          logDebug("MariaDB connection provider found")
          new MariaDBConnectionProvider(driver, options)

        case _ =>
          throw new IllegalArgumentException(s"Driver ${options.driverClass} does not support " +
            "Kerberos authentication")
      }
    }
  }
}
