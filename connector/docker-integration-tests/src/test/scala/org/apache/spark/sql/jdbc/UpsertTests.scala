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
import org.apache.spark.sql.functions.{col, lit, rand, when}

trait UpsertTests {
  self: DockerJDBCIntegrationSuite =>

  import testImplicits._

  def createTableOption: String
  def upsertTestOptions: Map[String, String] = Map("createTableOptions" -> createTableOption)

  test(s"Upsert existing table") { doTestUpsert(tableExists = true) }
  test(s"Upsert non-existing table") { doTestUpsert(tableExists = false) }

  Seq(
    Seq("ts", "id", "v1", "v2"),
    Seq("ts", "v1", "id", "v2"),
    Seq("ts", "v1", "v2", "id"),
    Seq("v2", "v1", "ts", "id"),
  ).foreach { columns =>
    test(s"Upsert with varying column order - ${columns.mkString(",")}") {
      doTestUpsert(tableExists = true, Some(columns))
    }
  }

  def doTestUpsert(tableExists: Boolean, project: Option[Seq[String]] = None): Unit = {
    val df = Seq(
      (1, Timestamp.valueOf("1996-01-01 01:23:46"), 1.235, 1.234568), // row unchanged
      (2, Timestamp.valueOf("1996-01-01 01:23:45"), 2.346, 2.345678), // updates v1
      (2, Timestamp.valueOf("1996-01-01 01:23:46"), 2.347, 2.345680), // updates v1 and v2
      (3, Timestamp.valueOf("1996-01-01 01:23:45"), 3.456, 3.456789) // inserts new row
    ).toDF("id", "ts", "v1", "v2").repartition(1) // .repartition(10)

    val table = if (tableExists) "upsert" else "new_upsert_table"
    val options = upsertTestOptions ++ Map(
      "numPartitions" -> "10",
      "upsert" -> "true",
      "upsertKeyColumns" -> "id, ts"
    )
    project.map(columns => df.select(columns.map(col): _*)).getOrElse(df)
      .write.mode(SaveMode.Append).options(options).jdbc(jdbcUrl, table, new Properties)

    val actual = spark.read.jdbc(jdbcUrl, table, new Properties).collect().toSet
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

  test(s"Upsert concurrency") {
    // create a table with 100k rows
    val init =
      spark.range(100000)
        .withColumn("ts", lit(Timestamp.valueOf("2023-06-07 12:34:56")))
        .withColumn("v", rand())

    // upsert 100 batches of 100 rows each
    // run in 32 tasks
    val patch =
      spark.range(100)
        .join(spark.range(100).select(($"id" * 1000).as("offset")))
        .repartition(32)
        .select(
          ($"id" + $"offset").as("id"),
          lit(Timestamp.valueOf("2023-06-07 12:34:56")).as("ts"),
          lit(-1.0).as("v")
        )

    spark.sparkContext.setJobDescription("init")
    init
      .write
      .mode(SaveMode.Overwrite)
      .option("createTableOptions", createTableOption)
      .jdbc(jdbcUrl, "new_upsert_table", new Properties)

    spark.sparkContext.setJobDescription("patch")
    patch
      .write
      .mode(SaveMode.Append)
      .option("upsert", value = true)
      .option("upsertKeyColumns", "id, ts")
      .options(upsertTestOptions)
      .jdbc(jdbcUrl, "new_upsert_table", new Properties)

    // check result table has 100*100 updated rows
    val result = spark.read.jdbc(jdbcUrl, "new_upsert_table", new Properties)
      .select($"id", when($"v" === -1.0, true).otherwise(false).as("patched"))
      .groupBy($"patched")
      .count()
      .sort($"patched")
      .as[(Boolean, Long)]
      .collect()
    assert(result === Seq((false, 90000), (true, 10000)))
  }

  test("Upsert with columns that require quotes") {}
  test("Upsert with table name that requires quotes") {}
  test("Upsert null values") {}

  test("Write with unspecified mode with upsert") {}
  test("Write with overwrite mode with upsert") {}
  test("Write with error-if-exists mode with upsert") {}
  test("Write with ignore mode with upsert") {}
}
