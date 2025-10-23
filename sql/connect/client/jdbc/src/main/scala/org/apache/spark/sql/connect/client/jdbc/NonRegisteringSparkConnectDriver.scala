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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.{Connection, Driver, DriverPropertyInfo, SQLException, SQLFeatureNotSupportedException}
import java.util.Properties
import java.util.logging.Logger

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.util.VersionUtils

class NonRegisteringSparkConnectDriver extends Driver {

  override def acceptsURL(url: String): Boolean = url.startsWith("jdbc:sc://")

  override def connect(url: String, info: Properties): Connection = {
    if (url == null) {
      throw new SQLException("url must not be null")
    }

    if (this.acceptsURL(url)) new SparkConnectConnection(url, info) else null
  }

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
    Array.empty

  override def getMajorVersion: Int = VersionUtils.majorVersion(SPARK_VERSION)

  override def getMinorVersion: Int = VersionUtils.minorVersion(SPARK_VERSION)

  override def jdbcCompliant: Boolean = false

  override def getParentLogger: Logger = throw new SQLFeatureNotSupportedException
}
