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


import java.sql.DriverManager

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.sql.connect.client.jdbc.test.ConnectFunSuite
import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.client.jdbc.test.RemoteSparkSession
import org.apache.spark.sql.connect.client.jdbc.test.SQLHelper
import org.apache.spark.util.VersionUtils

class SparkConnectDriverSuite extends ConnectFunSuite with RemoteSparkSession
    with SQLHelper with JdbcHelper {

  override def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  test("test SparkConnectDriver") {
    assert(DriverManager.getDriver(jdbcUrl).isInstanceOf[SparkConnectDriver])

    withConnection { conn =>
      assert(conn.isInstanceOf[SparkConnectConnection])
    }
  }

  test("test SparkConnectDatabaseMetaData") {
    withConnection { conn =>
      val dbm = conn.getMetaData
      assert(dbm.getDatabaseProductName === "Apache Spark Connect Server")
      assert(dbm.getDatabaseProductVersion === SPARK_VERSION)
      assert(dbm.getDriverName === "Apache Spark Connect JDBC Driver")
      assert(dbm.getDriverVersion === SPARK_VERSION)
      assert(dbm.getDriverMajorVersion === VersionUtils.majorVersion(SPARK_VERSION))
      assert(dbm.getDriverMinorVersion === VersionUtils.minorVersion(SPARK_VERSION))
    }
  }
}
