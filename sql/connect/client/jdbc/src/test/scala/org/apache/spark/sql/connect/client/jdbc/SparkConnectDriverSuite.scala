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

import java.sql.{Array => _, _}
import java.util.Properties

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}
import org.apache.spark.util.VersionUtils

class SparkConnectDriverSuite extends ConnectFunSuite with RemoteSparkSession
    with JdbcHelper {

  def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  test("get Connection from SparkConnectDriver") {
    assert(DriverManager.getDriver(jdbcUrl).isInstanceOf[SparkConnectDriver])

    val cause = intercept[SQLException] {
      new SparkConnectDriver().connect(null, new Properties())
    }
    assert(cause.getMessage === "url must not be null")

    withConnection { conn =>
      assert(conn.isInstanceOf[SparkConnectConnection])
    }
  }

  test("get DatabaseMetaData from SparkConnectConnection") {
    withConnection { conn =>
      val spark = conn.asInstanceOf[SparkConnectConnection].spark
      val metadata = conn.getMetaData
      assert(metadata.getURL === jdbcUrl)
      assert(metadata.isReadOnly === false)
      assert(metadata.getUserName === spark.client.configuration.userName)
      assert(metadata.getDatabaseProductName === "Apache Spark Connect Server")
      assert(metadata.getDatabaseProductVersion === spark.version)
      assert(metadata.getDriverVersion === SPARK_VERSION)
      assert(metadata.getDriverMajorVersion === VersionUtils.majorVersion(SPARK_VERSION))
      assert(metadata.getDriverMinorVersion === VersionUtils.minorVersion(SPARK_VERSION))
      assert(metadata.getIdentifierQuoteString === "`")
      assert(metadata.getExtraNameCharacters === "")
      assert(metadata.supportsGroupBy === true)
      assert(metadata.supportsOuterJoins === true)
      assert(metadata.supportsFullOuterJoins === true)
      assert(metadata.supportsLimitedOuterJoins === true)
      assert(metadata.getSchemaTerm === "schema")
      assert(metadata.getProcedureTerm === "procedure")
      assert(metadata.getCatalogTerm === "catalog")
      assert(metadata.isCatalogAtStart === true)
      assert(metadata.getCatalogSeparator === ".")
      assert(metadata.getConnection === conn)
      assert(metadata.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT) === false)
      assert(metadata.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT) === true)
      assert(metadata.getResultSetHoldability === ResultSet.CLOSE_CURSORS_AT_COMMIT)
      assert(metadata.getDatabaseMajorVersion === VersionUtils.majorVersion(spark.version))
      assert(metadata.getDatabaseMinorVersion === VersionUtils.minorVersion(spark.version))
      assert(metadata.getJDBCMajorVersion === 4)
      assert(metadata.getJDBCMinorVersion === 3)
      assert(metadata.supportsStatementPooling === false)
      assert(metadata.getRowIdLifetime === RowIdLifetime.ROWID_UNSUPPORTED)
      assert(metadata.generatedKeyAlwaysReturned === false)
      assert(metadata.getMaxLogicalLobSize === 0)
      assert(metadata.supportsRefCursors === false)
      assert(metadata.supportsSharding === false)
    }
  }
}
