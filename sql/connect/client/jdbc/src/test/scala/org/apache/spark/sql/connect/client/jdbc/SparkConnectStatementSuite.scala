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

import scala.util.Using

import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession, SQLHelper}

class SparkConnectStatementSuite extends ConnectFunSuite with RemoteSparkSession
  with JdbcHelper with SQLHelper {

  override def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  test("returned result set and update count of execute* methods") {
    withTable("t1", "t2", "t3") {
      withStatement { stmt =>
        // CREATE TABLE
        assert(!stmt.execute("CREATE TABLE t1 (id INT) USING Parquet"))
        assert(stmt.getUpdateCount === 0)
        assert(stmt.getResultSet === null)

        var se = intercept[SQLException] {
          stmt.executeQuery("CREATE TABLE t2 (id INT) USING Parquet")
        }
        assert(se.getMessage === "The query does not produce a ResultSet.")

        assert(stmt.executeUpdate("CREATE TABLE t3 (id INT) USING Parquet") === 0)
        assert(stmt.getResultSet === null)

        // INSERT INTO
        assert(!stmt.execute("INSERT INTO t1 VALUES (1)"))
        assert(stmt.getUpdateCount === 0)
        assert(stmt.getResultSet === null)

        se = intercept[SQLException] {
          stmt.executeQuery("INSERT INTO t1 VALUES (1)")
        }
        assert(se.getMessage === "The query does not produce a ResultSet.")

        assert(stmt.executeUpdate("INSERT INTO t1 VALUES (1)") === 0)
        assert(stmt.getResultSet === null)

        // SELECT
        assert(stmt.execute("SELECT id FROM t1"))
        assert(stmt.getUpdateCount === -1)
        Using.resource(stmt.getResultSet) { rs =>
          assert(rs !== null)
        }

        Using.resource(stmt.executeQuery("SELECT id FROM t1")) { rs =>
          assert(stmt.getUpdateCount === -1)
          assert(rs !== null)
        }

        se = intercept[SQLException] {
          stmt.executeUpdate("SELECT id FROM t1")
        }
        assert(se.getMessage === "The query produces a ResultSet.")
      }
    }
  }

  test("max rows from SparkConnectStatement") {
    def verifyMaxRows(
        expectedRows: Int, query: String)(stmt: Statement): Unit = {
      Using(stmt.executeQuery(query)) { rs =>
        (0 until expectedRows).foreach { _ =>
          assert(rs.next())
        }
        assert(!rs.next())
      }
    }

    withStatement { stmt =>
      // by default, it has no max rows limitation
      assert(stmt.getMaxRows === 0)
      verifyMaxRows(10, "SELECT id FROM range(10)")(stmt)

      val se = intercept[SQLException] {
        stmt.setMaxRows(-1)
      }
      assert(se.getMessage === "The max rows must be zero or a positive integer.")

      stmt.setMaxRows(5)
      assert(stmt.getMaxRows === 5)
      verifyMaxRows(5, "SELECT id FROM range(10)")(stmt)

      // set max rows for query that has LIMIT
      stmt.setMaxRows(5)
      assert(stmt.getMaxRows === 5)
      verifyMaxRows(3, "SELECT id FROM range(10) LIMIT 3")(stmt)
      verifyMaxRows(5, "SELECT id FROM range(10) LIMIT 8")(stmt)

      // set max rows for one statement won't affect others
      withStatement { stmt2 =>
        assert(stmt2.getMaxRows === 0)
        verifyMaxRows(10, "SELECT id FROM range(10)")(stmt2)
      }
    }
  }

  test("fetch size, fetch direction, result set type and query timeout accessors") {
    withStatement { stmt =>
      // fetch size: validated then silently dropped, always reads back as 0
      assert(stmt.getFetchSize === 0)
      stmt.setFetchSize(42)
      assert(stmt.getFetchSize === 0)
      val se1 = intercept[SQLException] {
        stmt.setFetchSize(-1)
      }
      assert(se1.getMessage === "Fetch size must be zero or a positive integer.")

      // fetch direction: only FETCH_FORWARD is supported
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD)
      assert(stmt.getFetchDirection === ResultSet.FETCH_FORWARD)
      intercept[SQLException] {
        stmt.setFetchDirection(ResultSet.FETCH_REVERSE)
      }

      // result set type is forward-only
      assert(stmt.getResultSetType === ResultSet.TYPE_FORWARD_ONLY)

      // query timeout: validated then silently dropped, always reads back as 0
      assert(stmt.getQueryTimeout === 0)
      stmt.setQueryTimeout(30)
      assert(stmt.getQueryTimeout === 0)
      val se2 = intercept[SQLException] {
        stmt.setQueryTimeout(-1)
      }
      assert(se2.getMessage === "Query timeout must be zero or a positive integer.")
    }
  }

  test("getMoreResults terminates JDBC drain loops") {
    // A typical JDBC result-draining loop. With getMoreResults throwing (or not
    // flipping getUpdateCount to -1) this would spin forever; assert it returns.
    def drain(stmt: Statement): Unit = {
      while (stmt.getMoreResults || stmt.getUpdateCount != -1) {}
    }

    withTable("t_drain") {
      withStatement { stmt =>
        // result-bearing command
        assert(stmt.execute("SELECT id FROM range(3)"))
        assert(stmt.getUpdateCount === -1)
        drain(stmt)
        assert(stmt.getResultSet === null)
        assert(stmt.getUpdateCount === -1)

        // result-less command
        assert(!stmt.execute("CREATE TABLE t_drain (id INT) USING Parquet"))
        assert(stmt.getUpdateCount === 0)
        drain(stmt)
        assert(stmt.getUpdateCount === -1)
      }
    }
  }
}
