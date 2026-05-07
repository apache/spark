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

  test("getTimestamp with multiple columns, rows, and types") {
    withExecuteQuery(
      """SELECT ts_tz, ts_ntz, id FROM VALUES
        |  (timestamp '2025-01-15 10:30:45.123456', timestamp_ntz '2025-06-20 14:22:33.789012', 1),
        |  (null, timestamp_ntz '2025-03-01 18:30:45.456789', 2),
        |  (timestamp '2025-10-31 23:59:59.999999', null, 3)
        |  AS t(ts_tz, ts_ntz, id)
        |""".stripMargin) { rs =>

      // Test findColumn
      assert(rs.findColumn("ts_tz") === 1)
      assert(rs.findColumn("ts_ntz") === 2)
      assert(rs.findColumn("id") === 3)

      // Verify metadata
      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 3)
      assert(metaData.getColumnTypeName(1) === "TIMESTAMP")
      assert(metaData.getColumnTypeName(2) === "TIMESTAMP_NTZ")

      // Row 1: Both timestamps have values
      assert(rs.next())
      assert(rs.getRow === 1)

      val ts1 = rs.getTimestamp(1)
      assert(ts1 !== null)
      assert(ts1 === java.sql.Timestamp.valueOf("2025-01-15 10:30:45.123456"))
      assert(!rs.wasNull)

      val tsNtz1 = rs.getTimestamp("ts_ntz")
      assert(tsNtz1 !== null)
      assert(tsNtz1 === java.sql.Timestamp.valueOf("2025-06-20 14:22:33.789012"))
      assert(!rs.wasNull)

      // Row 2: TIMESTAMP is null, TIMESTAMP_NTZ has value
      assert(rs.next())
      assert(rs.getRow === 2)

      val ts2 = rs.getTimestamp(1)
      assert(ts2 === null)
      assert(rs.wasNull)

      val tsNtz2 = rs.getTimestamp(2)
      assert(tsNtz2 !== null)
      assert(tsNtz2 === java.sql.Timestamp.valueOf("2025-03-01 18:30:45.456789"))
      assert(!rs.wasNull)

      // Row 3: TIMESTAMP has value, TIMESTAMP_NTZ is null
      assert(rs.next())
      assert(rs.getRow === 3)

      val ts3 = rs.getTimestamp("ts_tz")
      assert(ts3 !== null)
      assert(ts3 === java.sql.Timestamp.valueOf("2025-10-31 23:59:59.999999"))
      assert(!rs.wasNull)

      val tsNtz3 = rs.getTimestamp(2)
      assert(tsNtz3 === null)
      assert(rs.wasNull)

      assert(!rs.next())
    }
  }
}
