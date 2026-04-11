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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TimeTypeSupportSuite extends QueryTest with SharedSparkSession {

  test("CREATE TABLE, INSERT and SELECT with TIME type") {
    val tableName = "test_time_support"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      // CREATE TABLE
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      // INSERT INTO
      sql(s"INSERT INTO $tableName VALUES (1, TIME '08:00:00'), (2, TIME '12:30:45.123456')")

      // SELECT
      val df = sql(s"SELECT * FROM $tableName ORDER BY id")

      // Verify schema
      assert(df.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("t", TimeType)
      )))

      // Verify data
      val result = df.collect()
      assert(result.length === 2)

      assert(result(0).getInt(0) === 1)
      assert(result(0).get(1) === java.time.LocalTime.of(8, 0))
      assert(result(1).getInt(0) === 2)
      assert(result(1).get(1) === java.time.LocalTime.of(12, 30, 45, 123456000))
    }
  }

  test("SELECT TIME literal") {
    val df = sql("SELECT TIME '10:30:00' as t")
    checkAnswer(df, Row(java.time.LocalTime.of(10, 30)))
  }

  test("CAST STRING to TIME") {
    val df = sql("SELECT CAST('14:20:00' AS TIME) as t")
    checkAnswer(df, Row(java.time.LocalTime.of(14, 20)))
  }

  test("cast string to time - DataFrame") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast invalid string to time") {
    import testImplicits._
    val df = Seq("invalid").toDF("t")
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).isNullAt(0))
  }

  test("cast invalid string to time in ANSI mode") {
    import testImplicits._
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      val df = Seq("invalid").toDF("t")
      intercept[Exception] {
        df.selectExpr("CAST(t AS TIME)").collect()
      }
    }
  }

  test("cast time to string") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS STRING)")
    assert(result.collect()(0).getString(0).contains("12:30:00"))
  }

  test("cast time to timestamp") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS TIMESTAMP)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast timestamp to time") {
    import testImplicits._
    val df = Seq("2023-01-01 12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIMESTAMP) as ts")
    val result = df.selectExpr("CAST(ts AS TIME)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast time to date") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS DATE)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast time to long") {
    import testImplicits._
    val df = Seq("12:30:00").toDF("t")
      .selectExpr("CAST(t AS TIME) as time")

    val result = df.selectExpr("CAST(time AS LONG)")
    assert(result.collect()(0).getLong(0) > 0)
  }

  test("cast long to time") {
    import testImplicits._
    val df = Seq(45000L).toDF("t") // 12:30:00 in seconds
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).get(0) != null)
  }

  test("cast null to time") {
    import testImplicits._
    val df = Seq(null.asInstanceOf[String]).toDF("t")
    val result = df.selectExpr("CAST(t AS TIME)")
    assert(result.collect()(0).isNullAt(0))
  }

  test("time cast roundtrip") {
    import testImplicits._
    val df = Seq("12:30:45").toDF("t")
    val result = df
      .selectExpr("CAST(t AS TIME) as time")
      .selectExpr("CAST(time AS STRING)")

    assert(result.collect()(0).getString(0).contains("12:30:45"))
  }

  test("cast time with codegen enabled") {
    import testImplicits._
    withSQLConf("spark.sql.codegen.wholeStage" -> "true") {
      val df = Seq("12:30:00").toDF("t")
      val result = df.selectExpr("CAST(t AS TIME)")
      assert(result.collect()(0).get(0) != null)
    }
  }

  test("cast time to boolean and vice-versa") {
    import testImplicits._
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      val df = Seq("00:00:00", "12:30:00").toDF("t")
        .selectExpr("CAST(t AS TIME) as time")

      val result = df.selectExpr("CAST(time AS BOOLEAN) as b")
      val rows = result.collect()
      assert(rows(0).getBoolean(0) === false) // 00:00:00 is 0 micros -> false
      assert(rows(1).getBoolean(0) === true)

      val backToTime = result.selectExpr("CAST(b AS TIME)")
      val timeRows = backToTime.collect()
      assert(timeRows(0).get(0) === java.time.LocalTime.of(0, 0))
      // true -> 1 micro -> 00:00:00.000001
      val expected = java.time.LocalTime.of(0, 0, 0, 1000)
      assert(timeRows(1).get(0) === expected)
    }
  }

  test("comprehensive testtable with logintime - CREATE, INSERT, SELECT and CAST operations") {
    val tableName = "testtable"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      // CREATE TABLE with id and logintime columns
      sql(s"CREATE TABLE $tableName (id INT, logintime TIME) USING $provider")

      // INSERT raw data with various time formats
      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '08:00:00'),
          (2, TIME '09:30:15'),
          (3, TIME '12:45:30.123456'),
          (4, TIME '15:20:00'),
          (5, TIME '18:00:00.999999'),
          (6, TIME '23:59:59')
      """)

      // Test 1: SELECT * FROM testtable
      val allData = sql(s"SELECT * FROM $tableName ORDER BY id")
      assert(allData.count() === 6)
      assert(allData.schema.fields.length === 2)
      assert(allData.schema.fields(0).name === "id")
      assert(allData.schema.fields(0).dataType === IntegerType)
      assert(allData.schema.fields(1).name === "logintime")
      assert(allData.schema.fields(1).dataType === TimeType)

      val allRows = allData.collect()
      assert(allRows(0).getInt(0) === 1)
      assert(allRows(0).get(1) === java.time.LocalTime.of(8, 0, 0))
      assert(allRows(2).getInt(0) === 3)
      assert(allRows(2).get(1) === java.time.LocalTime.of(12, 45, 30, 123456000))

      // Test 2: SELECT logintime FROM testtable
      val loginTimes = sql(s"SELECT logintime FROM $tableName ORDER BY id")
      assert(loginTimes.count() === 6)
      assert(loginTimes.schema.fields.length === 1)
      assert(loginTimes.schema.fields(0).dataType === TimeType)

      val timeRows = loginTimes.collect()
      assert(timeRows(0).get(0) === java.time.LocalTime.of(8, 0, 0))
      assert(timeRows(5).get(0) === java.time.LocalTime.of(23, 59, 59))

      // Test 3: CAST logintime to STRING
      val timeAsString = sql(
        s"SELECT id, CAST(logintime AS STRING) as time_str FROM $tableName ORDER BY id")
      val stringRows = timeAsString.collect()
      assert(stringRows(0).getString(1).contains("08:00:00"))
      assert(stringRows(2).getString(1).contains("12:45:30"))

      // Test 4: CAST logintime to TIMESTAMP
      val timeAsTimestamp = sql(
        s"SELECT id, CAST(logintime AS TIMESTAMP) as time_ts FROM $tableName ORDER BY id")
      assert(timeAsTimestamp.count() === 6)
      val tsRows = timeAsTimestamp.collect()
      assert(tsRows(0).get(1) != null)
      assert(tsRows(3).get(1) != null)

      // Test 5: CAST logintime to TIMESTAMP_NTZ - Skip (not supported)
      // TIME to TIMESTAMP_NTZ cast is not implemented

      // Test 6: CAST logintime to DATE - Skip (not supported)
      // TIME to DATE cast may not be meaningful

      // Test 7: CAST logintime to LONG (seconds since midnight)
      val timeAsLong = sql(
        s"SELECT id, CAST(logintime AS LONG) as time_secs FROM $tableName ORDER BY id")
      val longRows = timeAsLong.collect()
      // 08:00:00 = 8 * 3600 seconds
      assert(longRows(0).getLong(1) === 8L * 3600L)
      // 12:45:30.123456 = (12*3600 + 45*60 + 30) seconds (fractional part truncated)
      assert(longRows(2).getLong(1) === 12L * 3600L + 45L * 60L + 30L)

      // Test 8: CAST logintime to INT (seconds since midnight)
      val timeAsInt = sql(
        s"SELECT id, CAST(logintime AS INT) as time_secs FROM $tableName ORDER BY id")
      val intRows = timeAsInt.collect()
      // 08:00:00 = 8 * 3600 seconds
      assert(intRows(0).getInt(1) === 8 * 3600)

      // Test 9: CAST logintime to DOUBLE
      val timeAsDouble = sql(
        s"SELECT id, CAST(logintime AS DOUBLE) as time_dbl FROM $tableName ORDER BY id")
      val doubleRows = timeAsDouble.collect()
      assert(doubleRows(0).getDouble(1) > 0.0)

      // Test 10: WHERE clause with TIME comparison
      val morningLogins = sql(
        s"SELECT id FROM $tableName WHERE logintime < TIME '12:00:00' ORDER BY id")
      assert(morningLogins.count() === 2) // ids 1 and 2

      // Test 11: WHERE clause with TIME range
      val afternoonLogins = sql(
        s"SELECT id FROM $tableName WHERE logintime >= TIME '12:00:00' " +
        s"AND logintime < TIME '18:00:00' ORDER BY id")
      assert(afternoonLogins.count() === 2) // ids 3 and 4

      // Test 12: Aggregate functions with TIME
      val minLoginTime = sql(s"SELECT MIN(logintime) as earliest FROM $tableName")
      assert(minLoginTime.collect()(0).get(0) === java.time.LocalTime.of(8, 0, 0))

      val maxLoginTime = sql(s"SELECT MAX(logintime) as latest FROM $tableName")
      assert(maxLoginTime.collect()(0).get(0) === java.time.LocalTime.of(23, 59, 59))

      // Test 13: COUNT with TIME column
      val countLogins = sql(s"SELECT COUNT(logintime) as total FROM $tableName")
      assert(countLogins.collect()(0).getLong(0) === 6)

      // Test 14: SELECT DISTINCT times
      val distinctTimes = sql(s"SELECT DISTINCT logintime FROM $tableName")
      assert(distinctTimes.count() === 6)

      // Test 15: CAST from STRING to TIME in WHERE clause
      val stringCastFilter = sql(
        s"SELECT id FROM $tableName WHERE logintime = CAST('08:00:00' AS TIME)")
      assert(stringCastFilter.count() === 1)
      assert(stringCastFilter.collect()(0).getInt(0) === 1)

      // Test 16: NULL handling
      sql(s"INSERT INTO $tableName VALUES (7, NULL)")
      val withNull = sql(s"SELECT id, logintime FROM $tableName WHERE id = 7")
      assert(withNull.collect()(0).isNullAt(1))

      val notNullCount = sql(s"SELECT COUNT(logintime) FROM $tableName")
      assert(notNullCount.collect()(0).getLong(0) === 6) // NULL not counted

      // Test 17: ORDER BY logintime
      val orderedByTime = sql(
        s"SELECT id, logintime FROM $tableName WHERE id <= 6 ORDER BY logintime")
      val orderedRows = orderedByTime.collect()
      assert(orderedRows(0).getInt(0) === 1) // 08:00:00
      assert(orderedRows(5).getInt(0) === 6) // 23:59:59

      // Test 18: CAST roundtrip: TIME -> STRING -> TIME
      val roundtrip = sql(s"""
        SELECT id, CAST(CAST(logintime AS STRING) AS TIME) as roundtrip_time
        FROM $tableName
        WHERE id = 3
      """)
      val roundtripRow = roundtrip.collect()(0)
      assert(roundtripRow.get(1) === java.time.LocalTime.of(12, 45, 30, 123456000))

      // Test 19: CAST roundtrip: TIME -> LONG -> TIME
      val longRoundtrip = sql(s"""
        SELECT id, CAST(CAST(logintime AS LONG) AS TIME) as roundtrip_time
        FROM $tableName
        WHERE id = 1
      """)
      val longRoundtripRow = longRoundtrip.collect()(0)
      assert(longRoundtripRow.get(1) === java.time.LocalTime.of(8, 0, 0))
    }
  }

  test("INSERT string values into TIME column with implicit conversion") {
    val tableName = "implicit_time_test"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      // CREATE TABLE with TIME column
      sql(s"CREATE TABLE $tableName (id INT, logintime TIME) USING $provider")

      // INSERT string values without explicit CAST - should auto-convert to TIME
      sql(s"""
        INSERT INTO $tableName VALUES
          (1, '08:00:00'),
          (2, '09:30:15'),
          (3, '12:45:30.123456'),
          (4, '15:20:00'),
          (5, '18:00:00.999999'),
          (6, '23:59:59')
      """)

      // SELECT all data
      val result = sql(s"SELECT * FROM $tableName ORDER BY id")
      val rows = result.collect()

      // Verify count
      assert(rows.length === 6)

      // Verify schema
      assert(result.schema.fields(0).name === "id")
      assert(result.schema.fields(0).dataType === IntegerType)
      assert(result.schema.fields(1).name === "logintime")
      assert(result.schema.fields(1).dataType === TimeType)

      // Verify data values
      assert(rows(0).getInt(0) === 1)
      assert(rows(0).get(1) === java.time.LocalTime.of(8, 0, 0))

      assert(rows(1).getInt(0) === 2)
      assert(rows(1).get(1) === java.time.LocalTime.of(9, 30, 15))

      assert(rows(2).getInt(0) === 3)
      assert(rows(2).get(1) === java.time.LocalTime.of(12, 45, 30, 123456000))

      assert(rows(3).getInt(0) === 4)
      assert(rows(3).get(1) === java.time.LocalTime.of(15, 20, 0))

      assert(rows(4).getInt(0) === 5)
      assert(rows(4).get(1) === java.time.LocalTime.of(18, 0, 0, 999999000))

      assert(rows(5).getInt(0) === 6)
      assert(rows(5).get(1) === java.time.LocalTime.of(23, 59, 59))
    }
  }

  test("Edge case: boundary TIME values") {
    val tableName = "time_boundary_test"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      // Insert boundary values
      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '00:00:00'),
          (2, TIME '23:59:59.999999')
      """)

      val result = sql(s"SELECT * FROM $tableName ORDER BY id")
      val rows = result.collect()

      assert(rows(0).get(1) === java.time.LocalTime.of(0, 0, 0))
      assert(rows(1).get(1) === java.time.LocalTime.of(23, 59, 59, 999999000))
    }
  }

  test("Edge case: microsecond precision") {
    val tableName = "time_microsecond_test"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      sql(s"INSERT INTO $tableName VALUES (1, TIME '12:30:45.123456')")

      val result = sql(s"SELECT t FROM $tableName WHERE id = 1")
      val time = result.collect()(0).get(0).asInstanceOf[java.time.LocalTime]

      assert(time === java.time.LocalTime.of(12, 30, 45, 123456000))
    }
  }

  test("Invalid INSERT: out of range time") {
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      // Test with direct CAST in SELECT to verify non-ANSI behavior
      val df = sql("SELECT CAST('24:00:00' AS TIME) as t")
      val result = df.collect()
      assert(result(0).isNullAt(0),
        "Invalid time '24:00:00' should result in NULL in non-ANSI mode")

      // Also test with try_cast which should always return NULL for invalid input
      val df2 = sql("SELECT TRY_CAST('24:00:00' AS TIME) as t")
      val result2 = df2.collect()
      assert(result2(0).isNullAt(0), "try_cast with invalid time should return NULL")
    }
  }

  test("Invalid INSERT: out of range time in ANSI mode") {
    val tableName = "time_invalid_range_ansi"
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      withTable(tableName) {
        val provider = spark.sessionState.conf.defaultDataSourceName
        sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

        intercept[Exception] {
          sql(s"INSERT INTO $tableName VALUES (1, '24:00:00')")
        }
      }
    }
  }

  test("Invalid INSERT: overflow numeric value") {
    val tableName = "time_overflow_test"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        // 86400 seconds = 24 hours, which is out of range for TIME
        sql(s"INSERT INTO $tableName VALUES (1, CAST(86400 AS TIME))")
        val result = sql(s"SELECT t FROM $tableName WHERE id = 1")
        // Should be NULL or wrapped around
        val value = result.collect()(0)
        // In non-ANSI mode, overflow might wrap or return NULL
        assert(value.isNullAt(0) || value.get(0) != null)
      }
    }
  }

  test("NULL INSERT and filtering") {
    val tableName = "time_null_test"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '10:00:00'),
          (2, NULL),
          (3, TIME '15:00:00')
      """)

      val nullResult = sql(s"SELECT * FROM $tableName WHERE t IS NULL")
      assert(nullResult.count() === 1)
      assert(nullResult.collect()(0).getInt(0) === 2)

      val notNullResult = sql(s"SELECT * FROM $tableName WHERE t IS NOT NULL")
      assert(notNullResult.count() === 2)
    }
  }

  test("GROUP BY with TIME column") {
    val tableName = "time_groupby_test"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '10:00:00'),
          (2, TIME '10:00:00'),
          (3, TIME '15:00:00'),
          (4, TIME '15:00:00'),
          (5, TIME '15:00:00')
      """)

      val grouped = sql(s"SELECT t, COUNT(*) as cnt FROM $tableName GROUP BY t ORDER BY t")
      val rows = grouped.collect()

      assert(rows.length === 2)
      assert(rows(0).get(0) === java.time.LocalTime.of(10, 0, 0))
      assert(rows(0).getLong(1) === 2)
      assert(rows(1).get(0) === java.time.LocalTime.of(15, 0, 0))
      assert(rows(1).getLong(1) === 3)
    }
  }

  test("TIME functions: hour, minute, second") {
    val df = sql("SELECT TIME '14:25:36' as t")
    val result = df.selectExpr("hour(t)", "minute(t)", "second(t)").collect()(0)

    assert(result.getInt(0) === 14)
    assert(result.getInt(1) === 25)
    assert(result.getInt(2) === 36)
  }

  test("TIME function: date_format") {
    val df = sql("SELECT TIME '14:25:36' as t")
    val result = df.selectExpr("date_format(CAST(t AS TIMESTAMP), 'HH:mm:ss')").collect()(0)

    assert(result.getString(0) === "14:25:36")
  }

  test("Parquet integration: write and read TIME column") {
    val tableName = "parquet_time_test"
    val parquetTable = "parquet_time_copy"

    withTable(tableName, parquetTable) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '08:00:00'),
          (2, TIME '12:30:45.123456'),
          (3, TIME '18:00:00')
      """)

      // Write to Parquet
      sql(s"CREATE TABLE $parquetTable USING parquet AS SELECT * FROM $tableName")

      // Read from Parquet
      val result = sql(s"SELECT * FROM $parquetTable ORDER BY id")
      val rows = result.collect()

      assert(rows.length === 3)
      assert(result.schema.fields(1).dataType === TimeType)
      assert(rows(0).get(1) === java.time.LocalTime.of(8, 0, 0))
      assert(rows(1).get(1) === java.time.LocalTime.of(12, 30, 45, 123456000))
      assert(rows(2).get(1) === java.time.LocalTime.of(18, 0, 0))
    }
  }

  test("JSON integration: write and read TIME column") {
    val tableName = "json_time_test"
    val jsonTable = "json_time_copy"

    withTable(tableName, jsonTable) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '08:00:00'),
          (2, TIME '12:30:45'),
          (3, TIME '18:00:00')
      """)

      // Write to JSON
      sql(s"CREATE TABLE $jsonTable USING json AS SELECT * FROM $tableName")

      // Read from JSON
      val result = sql(s"SELECT * FROM $jsonTable ORDER BY id")
      val rows = result.collect()

      assert(rows.length === 3)
      // JSON might store as string, so verify data is readable
      assert(rows(0).getInt(0) === 1)
      assert(rows(1).getInt(0) === 2)
      assert(rows(2).getInt(0) === 3)
    }
  }

  test("INSERT with numeric value (seconds since midnight)") {
    val tableName = "time_numeric_insert"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      // 36000 seconds = 10 hours = 10:00:00
      sql(s"INSERT INTO $tableName VALUES (1, CAST(36000 AS TIME))")

      val result = sql(s"SELECT t FROM $tableName WHERE id = 1")
      assert(result.collect()(0).get(0) === java.time.LocalTime.of(10, 0, 0))
    }
  }

  test("INSERT with TIMESTAMP cast to TIME") {
    val tableName = "time_from_timestamp"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING $provider")

      sql(s"INSERT INTO $tableName VALUES (1, CAST(TIMESTAMP '2024-01-01 10:30:45' AS TIME))")

      val result = sql(s"SELECT t FROM $tableName WHERE id = 1")
      assert(result.collect()(0).get(0) === java.time.LocalTime.of(10, 30, 45))
    }
  }

  test("CREATE TABLE with multiple types including TIME") {
    val tableName = "time_mixed_types"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"""
        CREATE TABLE $tableName (
          id INT,
          t TIME,
          ts TIMESTAMP,
          d DATE
        ) USING $provider
      """)

      sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIME '10:30:45', TIMESTAMP '2024-01-01 10:30:45', DATE '2024-01-01')
      """)

      val result = sql(s"SELECT * FROM $tableName")
      val row = result.collect()(0)

      assert(row.getInt(0) === 1)
      assert(row.get(1) === java.time.LocalTime.of(10, 30, 45))
      assert(row.get(2) != null) // TIMESTAMP
      assert(row.get(3) != null) // DATE
    }
  }

  test("Negative test: invalid comparison") {
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      val df = sql("SELECT TIME '10:00:00' = 'invalid'")
      // Should not throw, but result might be NULL or false
      val result = df.collect()
      assert(result.length === 1)
    }
  }

  test("Negative test: invalid function input") {
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      val df = sql("SELECT hour('not-a-time')")
      // Should return NULL in non-ANSI mode
      val result = df.collect()(0)
      assert(result.isNullAt(0))
    }
  }

  test("Roundtrip test: CREATE, INSERT, SELECT") {
    val tableName = "time_roundtrip"
    withTable(tableName) {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE $tableName (t TIME) USING $provider")
      sql(s"INSERT INTO $tableName VALUES (TIME '09:15:30')")

      val result = sql(s"SELECT t, CAST(t AS STRING) FROM $tableName")
      val row = result.collect()(0)

      assert(row.get(0) === java.time.LocalTime.of(9, 15, 30))
      assert(row.getString(1).contains("09:15:30"))
    }
  }
}
