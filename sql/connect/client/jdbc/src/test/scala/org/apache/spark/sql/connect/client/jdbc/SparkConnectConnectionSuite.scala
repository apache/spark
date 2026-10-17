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

import java.sql.SQLException

import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class SparkConnectConnectionSuite extends ConnectFunSuite with RemoteSparkSession
    with JdbcHelper {

  override def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  test("isValid checks the server, including when the timeout is zero") {
    withConnection { connection =>
      assert(connection.isValid(0))
      assert(connection.isValid(5))
    }
  }

  test("isValid rejects a negative timeout") {
    withConnection { connection =>
      intercept[SQLException] { connection.isValid(-1) }
    }
  }

  test("isValid returns false for a closed connection") {
    withConnection { connection =>
      connection.close()
      assert(!connection.isValid(5))
    }
  }
}
