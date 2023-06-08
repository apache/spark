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

package org.apache.spark.sql.jdbc

import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.sql.{Row, SaveMode}

trait UpsertTests {
  self: DockerJDBCIntegrationSuite =>

  import testImplicits._

  test(s"Upsert existing table") { doTestUpsert(true) }
  test(s"Upsert non-existing table") { doTestUpsert(false) }

  def doTestUpsert(tableExists: Boolean): Unit = {
    val df = Seq(
      (1, Timestamp.valueOf("1996-01-01 01:23:46"), 1.235, 1.234568), // row unchanged
      (2, Timestamp.valueOf("1996-01-01 01:23:45"), 2.346, 2.345678), // updates v1
      (2, Timestamp.valueOf("1996-01-01 01:23:46"), 2.347, 2.345680), // updates v1 and v2
      (3, Timestamp.valueOf("1996-01-01 01:23:45"), 3.456, 3.456789) // inserts new row
    ).toDF("id", "ts", "v1", "v2").repartition(1) // .repartition(10)

    val table = if (tableExists) "upsert" else "new_table"
    val options = Map("numPartitions" -> "10", "upsert" -> "true", "upsertKeyColumns" -> "id, ts")
    df.write.mode(SaveMode.Append).options(options).jdbc(jdbcUrl, table, new Properties)

    val actual = spark.read.jdbc(jdbcUrl, table, new Properties).collect.toSet
    val existing = if (tableExists) {
      Set((1, Timestamp.valueOf("1996-01-01 01:23:45"), 1.234, 1.234567))
    } else {
      Set.empty
    }
    val upsertedRows = Set(
      (1, Timestamp.valueOf("1996-01-01 01:23:46"), 1.235, 1.234568),
      (2, Timestamp.valueOf("1996-01-01 01:23:45"), 2.346, 2.345678),
      (2, Timestamp.valueOf("1996-01-01 01:23:46"), 2.347, 2.345680),
      (3, Timestamp.valueOf("1996-01-01 01:23:45"), 3.456, 3.456789)
    )
    val expected = (existing ++ upsertedRows).map { case (id, ts, v1, v2) =>
      Row(Integer.valueOf(id), ts, v1.doubleValue(), v2.doubleValue())
    }
    assert(actual === expected)
  }

  test("Upsert null values") {}
  test("Write with unspecified mode with upsert") {}
  test("Write with overwrite mode with upsert") {}
  test("Write with error-if-exists mode with upsert") {}
  test("Write with ignore mode with upsert") {}
}
