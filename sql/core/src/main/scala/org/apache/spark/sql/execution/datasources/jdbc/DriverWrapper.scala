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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties

import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * A wrapper for a JDBC Driver to work around SPARK-6913.
 *
 * The problem is in `java.sql.DriverManager` class that can't access drivers loaded by
 * Spark ClassLoader.
 */
class DriverWrapper(val wrapped: Driver) extends Driver {
  override def acceptsURL(url: String): Boolean = wrapped.acceptsURL(url)

  override def jdbcCompliant(): Boolean = wrapped.jdbcCompliant()

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    wrapped.getPropertyInfo(url, info)
  }

  override def getMinorVersion: Int = wrapped.getMinorVersion

  def getParentLogger: java.util.logging.Logger = {
    throw QueryExecutionErrors.getParentLoggerNotImplementedError(this.getClass.getName)
  }

  override def connect(url: String, info: Properties): Connection = wrapped.connect(url, info)

  override def getMajorVersion: Int = wrapped.getMajorVersion
}
