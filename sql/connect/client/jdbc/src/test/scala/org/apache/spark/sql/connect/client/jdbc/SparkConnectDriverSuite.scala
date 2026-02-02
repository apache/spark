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

import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

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
}
