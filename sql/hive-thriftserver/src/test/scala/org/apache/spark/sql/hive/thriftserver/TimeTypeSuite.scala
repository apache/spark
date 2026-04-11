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

package org.apache.spark.sql.hive.thriftserver

import org.apache.hive.service.rpc.thrift.TTypeId

import org.apache.spark.sql.types.TimeType

class TimeTypeSuite extends HiveThriftServer2Test {
  override def mode: ServerMode.Value = ServerMode.binary

  test("TimeType mapping to TTypeId") {
    // Test that TimeType maps to STRING_TYPE in Thrift
    assert(SparkExecuteStatementOperation.toTTypeId(TimeType) === TTypeId.STRING_TYPE)
  }

  test("TimeType mapping to java.sql.Types") {
    withJdbcStatement() { statement =>
      statement.execute("CREATE TEMPORARY VIEW time_test AS SELECT TIME '10:15:30' as t")

      val metaData = statement.getConnection.getMetaData
      val columns = metaData.getColumns(null, null, "time_test", "t")

      assert(columns.next())
      assert(columns.getInt("DATA_TYPE") === java.sql.Types.TIME)
      columns.close()
    }
  }

  test("TimeType query through JDBC") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT TIME '10:15:30' as time_col")
      assert(rs.next())
      
      // TimeType is represented as STRING in Thrift, so we get it as string
      val timeValue = rs.getString(1)
      assert(timeValue === "10:15:30")
      
      rs.close()
    }
  }

  test("TimeType with multiple values") {
    withJdbcStatement() { statement =>
      statement.execute(
        """CREATE TEMPORARY VIEW time_table AS
          |SELECT TIME '00:00:00' as t1,
          |       TIME '12:30:45' as t2,
          |       TIME '23:59:59' as t3
        """.stripMargin)

      val rs = statement.executeQuery("SELECT * FROM time_table")
      assert(rs.next())
      
      assert(rs.getString("t1") === "00:00:00")
      assert(rs.getString("t2") === "12:30:45")
      assert(rs.getString("t3") === "23:59:59")
      
      rs.close()
    }
  }

  test("TimeType column metadata") {
    withJdbcStatement() { statement =>
      statement.execute(
        "CREATE TEMPORARY VIEW time_metadata AS SELECT TIME '10:15:30' as time_column")

      val metaData = statement.getConnection.getMetaData
      val columns = metaData.getColumns(null, null, "time_metadata", null)

      assert(columns.next())
      assert(columns.getString("COLUMN_NAME") === "time_column")
      assert(columns.getInt("DATA_TYPE") === java.sql.Types.TIME)
      assert(columns.getString("TYPE_NAME") === "TIME")
      
      columns.close()
    }
  }

  test("TimeType with NULL values") {
    withJdbcStatement() { statement =>
      statement.execute(
        """CREATE TEMPORARY VIEW time_null AS
          |SELECT CAST(NULL AS TIME) as null_time,
          |       TIME '10:15:30' as valid_time
        """.stripMargin)

      val rs = statement.executeQuery("SELECT * FROM time_null")
      assert(rs.next())
      
      assert(rs.getString("null_time") === null)
      assert(rs.getString("valid_time") === "10:15:30")
      
      rs.close()
    }
  }

  test("TimeType in mixed type query") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery(
        """SELECT
          |  TIME '10:15:30' as time_col,
          |  DATE '2023-01-01' as date_col,
          |  TIMESTAMP '2023-01-01 10:15:30' as timestamp_col,
          |  'test' as string_col,
          |  123 as int_col
        """.stripMargin)

      assert(rs.next())
      
      val metaData = rs.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TIME)
      assert(metaData.getColumnType(2) === java.sql.Types.DATE)
      assert(metaData.getColumnType(3) === java.sql.Types.TIMESTAMP)
      assert(metaData.getColumnType(4) === java.sql.Types.VARCHAR)
      assert(metaData.getColumnType(5) === java.sql.Types.INTEGER)
      
      rs.close()
    }
  }
}
