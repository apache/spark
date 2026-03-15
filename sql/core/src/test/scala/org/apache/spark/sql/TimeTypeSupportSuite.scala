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
      // CREATE TABLE
      sql(s"CREATE TABLE $tableName (id INT, t TIME) USING parquet")

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
}
