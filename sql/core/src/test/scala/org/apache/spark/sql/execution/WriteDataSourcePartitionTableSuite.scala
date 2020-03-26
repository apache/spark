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

package org.apache.spark.sql.execution

import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class WriteDataSourcePartitionTableSuite extends WritePartitionTableSuite with SharedSparkSession {
  protected val tableProvider = "PARQUET"
}

abstract class WritePartitionTableSuite extends SQLTestUtils {

  protected val tableProvider: String

  test("CTAS with dynamic partitions") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          val df = sql(
            s"""CREATE TABLE t1 USING $tableProvider PARTITIONED BY (p1, p2)
               |AS (SELECT id, id AS  p1, id AS p2 FROM range(5))""".stripMargin)
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          if (repartitionBeforeInsert) {
            assert(partitionings.size === 1)
          } else {
            assert(partitionings.size === 0)
          }
        }
      }
    }
  }

  test("INSERT dynamic partitions") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
           s"""CREATE TABLE t1 (
              |  id BIGINT,
              |  p1 BIGINT,
              |  p2 BIGINT)
              |USING PARQUET
              |PARTITIONED BY (p1, p2)""".stripMargin)
          val df = sql("INSERT INTO t1 SELECT id, id AS  p1, id AS p2 FROM range(5)")
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          if (repartitionBeforeInsert) {
            assert(partitionings.size === 1)
          } else {
            assert(partitionings.size === 0)
          }
        }
      }
    }
  }

  test("INSERT static and dynamic partitions") {
    Seq(false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
            s"""CREATE TABLE t1 (
               |  id BIGINT,
               |  p1 BIGINT,
               |  p2 BIGINT)
               |USING PARQUET
               |PARTITIONED BY (p1, p2)""".stripMargin)
          val df = sql("INSERT INTO t1 PARTITION(p1 = 1, p2) SELECT id, id AS p2 FROM range(5)")
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          if (repartitionBeforeInsert) {
            assert(partitionings.size === 1)
          } else {
            assert(partitionings.size === 0)
          }
        }
      }
    }
  }

  test("INSERT static partitions") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
            s"""CREATE TABLE t1 (
               |  id BIGINT,
               |  p1 BIGINT,
               |  p2 BIGINT)
               |USING PARQUET
               |PARTITIONED BY (p1, p2)""".stripMargin)
          val df = sql("INSERT INTO t1 PARTITION(p1 = 1, p2 = 2) SELECT id FROM range(5)")
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          assert(partitionings.size === 0)
        }
      }
    }
  }

  test("INSERT static and dynamic partitions pre-partitioned by dynamic partition column") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
            s"""CREATE TABLE t1 (
               |  id BIGINT,
               |  p1 BIGINT,
               |  p2 BIGINT)
               |USING PARQUET
               |PARTITIONED BY (p1, p2)""".stripMargin)
          val df = sql(
            """INSERT INTO t1 PARTITION(p1 = 1, p2) SELECT id, id AS p2 FROM range(5)
              | DISTRIBUTE BY p2
              |""".stripMargin)
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          assert(partitionings.size === 1)
        }
      }
    }
  }

  test("INSERT dynamic partitions pre-partitioned by other column") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
            s"""CREATE TABLE t1 (
               |  id BIGINT,
               |  p1 BIGINT,
               |  p2 BIGINT)
               |USING PARQUET
               |PARTITIONED BY (p1, p2)""".stripMargin)
          val df = sql(
            """INSERT INTO t1 PARTITION(p1 = 1, p2) SELECT id, id AS p2 FROM range(5)
              | DISTRIBUTE BY id
              |""".stripMargin)
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          if (repartitionBeforeInsert) {
            assert(partitionings.size === 2)
          } else {
            assert(partitionings.size === 1)
          }
        }
      }
    }
  }

  test("INSERT upper case dynamic partitions pre-partitioned by other column") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
            s"""CREATE TABLE t1 (
               |  id BIGINT,
               |  p1 BIGINT,
               |  p2 BIGINT)
               |USING PARQUET
               |PARTITIONED BY (P1, p2)""".stripMargin)
          val df = sql(
            """INSERT INTO t1 PARTITION(P2, p1=3) SELECT id, cast(id as bigint) AS P2 FROM range(5)
              | DISTRIBUTE BY id
              |""".stripMargin)
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          if (repartitionBeforeInsert) {
            assert(partitionings.size === 2)
          } else {
            assert(partitionings.size === 1)
          }
        }
      }
    }
  }

  test("INSERT all static partitions") {
    Seq(true, false).foreach { repartitionBeforeInsert =>
      withSQLConf(
        SQLConf.REPARTITION_BEFORE_INSERT.key -> s"$repartitionBeforeInsert",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t1") {
          sql(
            s"""CREATE TABLE t1 (
               |  id BIGINT,
               |  p1 BIGINT,
               |  p2 BIGINT)
               |USING PARQUET
               |PARTITIONED BY (p1, p2)""".stripMargin)
          val df = sql(
            """INSERT INTO t1 PARTITION(p1 = 1, p2 = 2) SELECT id FROM range(5)
              |""".stripMargin)
          val partitionings = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(spark.table("t1").count() === 5)
          assert(partitionings.size === 0)
        }
      }
    }
  }

}
