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

package org.apache.spark.sql.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class InsertSchemaEvolutionSuite
    extends QueryTest with SharedSparkSession with BeforeAndAfter {

  private val catalogName = "testcat"
  private val namespace = "ns"
  private val tableIdent = s"$catalogName.$namespace.test_table"

  before {
    spark.conf.set(s"spark.sql.catalog.$catalogName", classOf[InMemoryCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalogName")
  }

  test("INSERT BY NAME with extra source column adds column to table") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")
      sql(
        s"""INSERT WITH SCHEMA EVOLUTION INTO $tableIdent BY NAME
           |SELECT * FROM VALUES (1, 'a', CAST(10.0 AS DOUBLE)),
           |  (2, 'b', CAST(20.0 AS DOUBLE)) AS t(id, data, amount)
           |""".stripMargin)

      val result = spark.table(tableIdent)
      checkAnswer(result, Seq(Row(1, "a", 10.0d), Row(2, "b", 20.0d)))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType),
        StructField("amount", DoubleType))))
    }
  }

  test("INSERT BY NAME with type widening updates column type") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, value INT)")
      sql(
        s"""INSERT WITH SCHEMA EVOLUTION INTO $tableIdent BY NAME
           |SELECT * FROM VALUES (1, CAST(100 AS LONG)),
           |  (2, CAST(200 AS LONG)) AS t(id, value)
           |""".stripMargin)

      val result = spark.table(tableIdent)
      checkAnswer(result, Seq(Row(1, 100L), Row(2, 200L)))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("value", LongType))))
    }
  }

  test("INSERT BY NAME with nested struct evolution") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, info STRUCT<name: STRING>)")
      sql(
        s"""INSERT WITH SCHEMA EVOLUTION INTO $tableIdent BY NAME
           |SELECT id, named_struct('name', name, 'age', age) AS info
           |FROM VALUES (1, 'Alice', 30), (2, 'Bob', 25) AS t(id, name, age)
           |""".stripMargin)

      val result = spark.table(tableIdent)
      checkAnswer(result, Seq(Row(1, Row("Alice", 30)), Row(2, Row("Bob", 25))))
      val expectedInfoType = StructType(Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("info", expectedInfoType))))
    }
  }

  test("INSERT BY NAME with matching schema - no evolution needed") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")
      sql(
        s"""INSERT WITH SCHEMA EVOLUTION INTO $tableIdent BY NAME
           |SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, data)
           |""".stripMargin)

      val result = spark.table(tableIdent)
      checkAnswer(result, Seq(Row(1, "a"), Row(2, "b")))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType))))
    }
  }

  test("INSERT BY POSITION with schema evolution adds extra columns") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")
      sql(
        s"""INSERT WITH SCHEMA EVOLUTION INTO $tableIdent
           |SELECT * FROM VALUES (1, 'a', CAST(10.0 AS DOUBLE)),
           |  (2, 'b', CAST(20.0 AS DOUBLE)) AS t(id, data, amount)
           |""".stripMargin)

      val result = spark.table(tableIdent)
      checkAnswer(result, Seq(Row(1, "a", 10.0d), Row(2, "b", 20.0d)))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType),
        StructField("amount", DoubleType))))
    }
  }

  test("table without AUTOMATIC_SCHEMA_EVOLUTION - no evolution") {
    withTable(tableIdent) {
      sql(
        s"""CREATE TABLE $tableIdent (id INT, data STRING)
           |TBLPROPERTIES ('auto-schema-evolution' = 'false')""".stripMargin)

      intercept[Exception] {
        sql(
          s"""INSERT WITH SCHEMA EVOLUTION INTO $tableIdent BY NAME
             |SELECT * FROM VALUES (1, 'a', CAST(10.0 AS DOUBLE)),
             |  (2, 'b', CAST(20.0 AS DOUBLE)) AS t(id, data, amount)
             |""".stripMargin)
      }
    }
  }

  test("OVERWRITE BY EXPRESSION with schema evolution") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, data STRING)")
      sql(s"INSERT INTO $tableIdent VALUES (1, 'a'), (2, 'b')")

      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        sql(
          s"""INSERT WITH SCHEMA EVOLUTION OVERWRITE $tableIdent BY NAME
             |SELECT * FROM VALUES (3, 'c', CAST(30.0 AS DOUBLE)),
             |  (4, 'd', CAST(40.0 AS DOUBLE)) AS t(id, data, amount)
             |""".stripMargin)
      }

      val result = spark.table(tableIdent)
      checkAnswer(result, Seq(Row(3, "c", 30.0d), Row(4, "d", 40.0d)))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType),
        StructField("amount", DoubleType))))
    }
  }

  test("OVERWRITE PARTITIONS DYNAMIC with schema evolution") {
    withTable(tableIdent) {
      sql(s"CREATE TABLE $tableIdent (id INT, data STRING) PARTITIONED BY (id)")
      sql(s"INSERT INTO $tableIdent VALUES (1, 'a'), (2, 'b')")

      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
        sql(
          s"""INSERT WITH SCHEMA EVOLUTION OVERWRITE $tableIdent BY NAME
             |SELECT * FROM VALUES (1, 'c', CAST(30.0 AS DOUBLE)) AS t(id, data, amount)
             |""".stripMargin)
      }

      val result = spark.table(tableIdent)
      checkAnswer(result.orderBy("id"),
        Seq(Row(1, "c", 30.0d), Row(2, "b", null)))
      assert(result.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType),
        StructField("amount", DoubleType))))
    }
  }
}
