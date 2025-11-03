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

import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class SparkConnectResultSetSuite extends ConnectFunSuite with RemoteSparkSession
  with JdbcHelper {

  def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  test("type, concurrency, fetch direction of result set") {
    withExecuteQuery("SELECT 1") { rs =>
      rs.getType === ResultSet.TYPE_FORWARD_ONLY
      rs.getConcurrency === ResultSet.CONCUR_READ_ONLY
      rs.getFetchDirection === ResultSet.FETCH_FORWARD
      Seq(ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE,
        ResultSet.FETCH_UNKNOWN).foreach { direction =>
        if (direction == ResultSet.FETCH_FORWARD) {
          rs.setFetchDirection(direction)
        } else {
          intercept[SQLException] {
            rs.setFetchDirection(direction)
          }
        }
      }
    }
  }

  test("row position for empty result set") {
    withExecuteQuery("SELECT * FROM range(0)") { rs =>
      assert(rs.getRow === 0)
      assert(!rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(!rs.isLast)
      assert(!rs.isAfterLast)

      assert(!rs.next())

      assert(rs.getRow === 0)
      assert(!rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(!rs.isLast)
      assert(!rs.isAfterLast)
    }
  }

  test("row position for one row result set") {
    withExecuteQuery("SELECT * FROM range(1)") { rs =>
      assert(rs.getRow === 0)
      assert(rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(!rs.isLast)
      assert(!rs.isAfterLast)

      assert(rs.next())

      assert(rs.getRow === 1)
      assert(!rs.isBeforeFirst)
      assert(rs.isFirst)
      assert(rs.isLast)
      assert(!rs.isAfterLast)

      assert(!rs.next())

      assert(rs.getRow === 0)
      assert(!rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(!rs.isLast)
      assert(rs.isAfterLast)
    }
  }

  test("row position for multiple rows result set") {
    withExecuteQuery("SELECT * FROM range(2)") { rs =>
      assert(rs.getRow === 0)
      assert(rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(!rs.isLast)
      assert(!rs.isAfterLast)

      assert(rs.next())

      assert(rs.getRow === 1)
      assert(!rs.isBeforeFirst)
      assert(rs.isFirst)
      assert(!rs.isLast)
      assert(!rs.isAfterLast)

      assert(rs.next())

      assert(rs.getRow === 2)
      assert(!rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(rs.isLast)
      assert(!rs.isAfterLast)

      assert(!rs.next())

      assert(rs.getRow === 0)
      assert(!rs.isBeforeFirst)
      assert(!rs.isFirst)
      assert(!rs.isLast)
      assert(rs.isAfterLast)
    }
  }
}
