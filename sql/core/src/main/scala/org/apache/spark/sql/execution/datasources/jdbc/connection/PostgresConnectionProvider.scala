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

import java.sql.Driver
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

private[jdbc] class PostgresConnectionProvider extends SecureConnectionProvider {
  override val driverClass = "org.postgresql.Driver"

  override val name: String = "postgres"

  override def appEntry(driver: Driver, options: JDBCOptions): String = {
    val parseURL = driver.getClass.getMethod("parseURL", classOf[String], classOf[Properties])
    val properties = parseURL.invoke(driver, options.url, null).asInstanceOf[Properties]
    properties.getProperty("jaasApplicationName", "pgjdbc")
  }
}
