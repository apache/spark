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

package org.apache.spark.sql.execution.dynamicpruning

import java.sql.{Date, Timestamp}

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Decimal, StringType}

abstract class ShufflePruningSuiteBase
  extends QueryTest
    with SharedSparkSession
    with GivenWhenThen
    with DynamicPruningHelp {

  val tableFormat: String = "parquet"

  val adaptiveExecutionOn: Boolean

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, adaptiveExecutionOn)
    spark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY, true)
    spark.sessionState.conf.setConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED, true)
    spark.sessionState.conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, 5000L)
    spark.sessionState.conf.setConf(SQLConf.PARQUET_COMPRESSION, "uncompressed")

    spark.range(2000)
      .select(col("id").as("a"), col("id").as("b"))
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t1")

    spark.range(2000)
      .select(col("id").as("a"), col("id").as("b"))
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t2")

    spark.range(100)
      .select(col("id").as("a"), col("id").as("b"))
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t3")

    spark.range(2000)
      .select(col("id").%(10).as("a"), col("id").as("b"))
      .write
      .partitionBy("a")
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t_part1")

    spark.range(2000)
      .select(col("id").as("a"), col("id").as("b"))
      .write
      .bucketBy(5, "a")
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t_bucket1")

    sql("ANALYZE TABLE t1 COMPUTE STATISTICS FOR ALL COLUMNS")
    sql("ANALYZE TABLE t2 COMPUTE STATISTICS FOR ALL COLUMNS")
    sql("ANALYZE TABLE t3 COMPUTE STATISTICS FOR ALL COLUMNS")
    sql("ANALYZE TABLE t_part1 COMPUTE STATISTICS FOR ALL COLUMNS")
    sql("ANALYZE TABLE t_bucket1 COMPUTE STATISTICS FOR ALL COLUMNS")
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS t1")
      sql("DROP TABLE IF EXISTS t2")
      sql("DROP TABLE IF EXISTS t3")
      sql("DROP TABLE IF EXISTS t_part1")
      sql("DROP TABLE IF EXISTS t_bucket1")
    } finally {
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY)
      spark.sessionState.conf.unsetConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
      spark.sessionState.conf.unsetConf(SQLConf.PARQUET_COMPRESSION)
      super.afterAll()
    }
  }

  test("check table stats") {
    val catalog = spark.sessionState.catalog
    assert(catalog.getTableMetadata(TableIdentifier("t1")).stats.get.sizeInBytes > 10000L)
    assert(catalog.getTableMetadata(TableIdentifier("t2")).stats.get.sizeInBytes > 10000L)
    assert(catalog.getTableMetadata(TableIdentifier("t3")).stats.get.sizeInBytes < 5000L)
    assert(catalog.getTableMetadata(TableIdentifier("t_part1")).stats.get.sizeInBytes > 10000L)
    assert(catalog.getTableMetadata(TableIdentifier("t_bucket1")).stats.get.sizeInBytes > 10000L)
  }

  test("simple aggregate triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.cnt
          |FROM   (SELECT a,
          |               Count(b) AS cnt
          |        FROM   t1
          |        GROUP  BY a) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, true)
      checkAnswer(df, Row(0, 1) :: Row(1, 1) :: Nil)
    }
  }

  test("CBO can evaluate row count more accurately") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K",
      SQLConf.DYNAMIC_PARTITION_PRUNING_BLOOM_FILTER_THRESHOLD.key -> "5") {
      Given("default bloom filter")
      withSQLConf(SQLConf.CBO_ENABLED.key -> "false", SQLConf.PLAN_STATS_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT t11.a,
            |       t11.cnt
            |FROM   (SELECT a,
            |               Count(b) AS cnt
            |        FROM   t1
            |        GROUP  BY a) t11
            |       JOIN t3
            |         ON t11.a = t3.a AND t3.b < 2
            |""".stripMargin)

        checkBloomFilterSubqueryPredicate(df, false, true)
        checkAnswer(df, Row(0, 1) :: Row(1, 1) :: Nil)
      }

      Given("default in")
      withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.PLAN_STATS_ENABLED.key -> "true") {
        val df = sql(
          """
            |SELECT t11.a,
            |       t11.cnt
            |FROM   (SELECT a,
            |               Count(b) AS cnt
            |        FROM   t1
            |        GROUP  BY a) t11
            |       JOIN t3
            |         ON t11.a = t3.a AND t3.b < 2
            |""".stripMargin)

        checkInSubqueryPredicate(df, false, true)
        checkAnswer(df, Row(0, 1) :: Row(1, 1) :: Nil)
      }
    }
  }

  test("dynamic filter push down to datasource") {
    withSQLConf(
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K",
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.PLAN_STATS_ENABLED.key -> "true") {
      Seq(true, false).foreach { pushdown =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> s"$pushdown") {
          val df = sql(
            """
              |SELECT t11.a,
              |       t11.cnt
              |FROM   (SELECT a,
              |               Count(b) AS cnt
              |        FROM   t1
              |        GROUP  BY a) t11
              |       JOIN t3
              |         ON t11.a = t3.a AND t3.b < 2
              |""".stripMargin)

          df.collect()
          val scan = getTableScan(df.queryExecution.executedPlan, "t1")
          assert(scan.metrics("numFiles").value === 2)
          if (pushdown) {
            assert(scan.metrics("numOutputRows").value === 1000)
          } else {
            assert(scan.metrics("numOutputRows").value === 2000)
          }
          checkInSubqueryPredicate(df, false, true)
          checkAnswer(df, Row(0, 1) :: Row(1, 1) :: Nil)
        }
      }
    }
  }

  test("union triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.b
          |FROM   (SELECT a,
          |               b
          |        FROM   t1
          |        WHERE  b < 10
          |        UNION
          |        SELECT a,
          |               b
          |        FROM   t1
          |        WHERE  b > 10) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, true)

      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("union all should not triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.b
          |FROM   (SELECT a,
          |               b
          |        FROM   t1
          |        WHERE  b < 10
          |        UNION ALL
          |        SELECT a,
          |               b
          |        FROM   t3
          |        WHERE  b > 10) t11
          |       JOIN t3
          |         ON t11.a = t3.a
          |            AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, false)

      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("SMJ should triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3K",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.b
          |FROM   (SELECT t1.a,
          |               t2.b
          |        FROM t1
          |        JOIN t2
          |            ON t1.a = t2.a) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, true)
      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("SMJ should not triggers shuffle pruning with exchange reuse enabled") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3K",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.b
          |FROM   (SELECT t1.a,
          |               t2.b
          |        FROM t1
          |        JOIN t2
          |            ON t1.a = t2.a) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, false)
      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("SMJ should triggers shuffle pruning with implicit type cast") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3K",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      withTable("t_string1") {
        spark.range(100)
          .select(col("id").cast(StringType).as("a"), col("id").as("b"))
          .write
          .format(tableFormat)
          .mode(SaveMode.Overwrite)
          .saveAsTable("t_string1")

        val df = sql(
          """
            |SELECT t11.a,
            |       t11.b
            |FROM   (SELECT t1.a,
            |               t2.b
            |        FROM t1
            |        JOIN t2
            |            ON t1.a = t2.a) t11
            |       JOIN t_string1
            |         ON t11.a = t_string1.a AND t_string1.b < 2
            |""".stripMargin)

        checkInSubqueryPredicate(df, false, true)
        checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
      }
    }
  }

  test("SMJ should triggers shuffle pruning with implicit type cast(empty result)") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3K",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      withTable("t_string1") {
        spark.range(100)
          .select(concat(col("id"), lit("str")).as("a"), col("id").as("b"))
          .write
          .format(tableFormat)
          .mode(SaveMode.Overwrite)
          .saveAsTable("t_string1")

        val df = sql(
          """
            |SELECT t11.a,
            |       t11.b
            |FROM   (SELECT t1.a,
            |               t2.b
            |        FROM t1
            |        JOIN t2
            |            ON t1.a = t2.a) t11
            |       JOIN t_string1
            |         ON t11.a = t_string1.a AND t_string1.b < 2
            |""".stripMargin)

        checkInSubqueryPredicate(df, false, true)
        checkAnswer(df, Nil)
      }
    }
  }

  private def checkSupportedDataTypes(value: Any): Unit = {
    test(s"Check support data type: ${value.getClass.getCanonicalName}") {
      withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
        withTable("t_dt1", "t_dt2") {
          spark.range(2000)
            .select(lit(Literal(value)).as("a"), col("id").as("b"))
            .write
            .format(tableFormat)
            .mode(SaveMode.Overwrite)
            .saveAsTable("t_dt1")

          spark.range(100)
            .select(lit(Literal(value)).as("a"), col("id").as("b"))
            .write
            .format(tableFormat)
            .mode(SaveMode.Overwrite)
            .saveAsTable("t_dt2")

          CodegenObjectFactoryMode.values.foreach { mode =>
            withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> mode.toString) {
              val df = sql(
                """
                  |SELECT t1.a,
                  |       t1.cnt
                  |FROM   (SELECT a,
                  |               Count(b) AS cnt
                  |        FROM   t_dt1
                  |        GROUP  BY a) t1
                  |       JOIN t_dt2 t2
                  |         ON t1.a = t2.a AND t2.b < 2
                  |""".stripMargin)

              checkInSubqueryPredicate(df, false, true)
              checkAnswer(df, Row(value, 2000) :: Row(value, 2000) :: Nil)
            }
          }
        }
      }
    }
  }

  checkSupportedDataTypes(false)
  checkSupportedDataTypes(1.toByte)
  checkSupportedDataTypes(2.toShort)
  checkSupportedDataTypes(3)
  checkSupportedDataTypes(4L)
  checkSupportedDataTypes(5.6.toFloat)
  checkSupportedDataTypes(7.8)
  checkSupportedDataTypes(new Decimal().set(9.0123).toPrecision(38, 18).toBigDecimal)
  checkSupportedDataTypes("str")
  checkSupportedDataTypes(Array[Byte](1, 2, 3, 4))
  checkSupportedDataTypes(Date.valueOf("2020-07-24"))
  checkSupportedDataTypes(Timestamp.valueOf("2020-07-24 14:01:11"))

  test("Support mutiple join keys") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      withTable("t_mj1", "t_mj2") {
        spark.range(2000)
          .select(col("id").as("a"), col("id").cast(StringType).as("b"), col("id").as("c"))
          .write
          .format(tableFormat)
          .mode(SaveMode.Overwrite)
          .saveAsTable("t_mj1")

        spark.range(100)
          .select(col("id").as("a"), col("id").cast(StringType).as("b"), col("id").as("c"))
          .write
          .format(tableFormat)
          .mode(SaveMode.Overwrite)
          .saveAsTable("t_mj2")

        val df = sql(
          """
            |SELECT t1.a,
            |       t1.cnt
            |FROM   (SELECT a,
            |               b,
            |               Count(c) AS cnt
            |        FROM   t_mj1
            |        GROUP  BY a, b) t1
            |       JOIN t_mj2 t2
            |         ON t1.a = t2.a AND t1.b = t2.b AND t2.c < 2
            |""".stripMargin)

        checkInSubqueryPredicate(df, false, true)
        checkDistinctSubqueries(df, 2)
        checkAnswer(df, Row(0, 1) :: Row(1, 1) :: Nil)
      }
    }
  }

  test("BHJ should not triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10M",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.b
          |FROM   (SELECT t1.a,
          |               t2.b
          |        FROM t1
          |        JOIN t2
          |            ON t1.a = t2.a) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, false)
      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("partition column should not triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.cnt
          |FROM   (SELECT a,
          |               Count(b) AS cnt
          |        FROM   t_part1
          |        GROUP  BY a) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, false)

      checkAnswer(df, Row(0, 200) :: Row(1, 200) :: Nil)
    }
  }

  test("bucket column should not triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      val df = sql(
        """
          |SELECT t11.a,
          |       t11.cnt
          |FROM   (SELECT a,
          |               Count(b) AS cnt
          |        FROM   t_bucket1
          |        GROUP  BY a) t11
          |       JOIN t3
          |         ON t11.a = t3.a AND t3.b < 2
          |""".stripMargin)

      checkInSubqueryPredicate(df, false, false)

      checkAnswer(df, Row(0, 1) :: Row(1, 1) :: Nil)
    }
  }

  test("array type triggers shuffle pruning") {
    withSQLConf(SQLConf.DYNAMIC_SHUFFLE_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_SHUFFLE_PRUNING_SIDE_THRESHOLD.key -> "10K") {
      withTable("t_array1", "t_array2") {
        spark.range(1000)
          .select(array(col("id")).as("a"), col("id").as("b"))
          .write
          .format(tableFormat)
          .mode(SaveMode.Overwrite)
          .saveAsTable("t_array1")

        spark.range(100)
          .select(array(col("id")).as("a"), col("id").as("b"))
          .write
          .format(tableFormat)
          .mode(SaveMode.Overwrite)
          .saveAsTable("t_array2")

        val df = sql(
          """
            |SELECT t1.a,
            |       t1.cnt
            |FROM   (SELECT a,
            |               Count(b) AS cnt
            |        FROM   t_array1
            |        GROUP  BY a) t1
            |       JOIN t_array2 t2
            |         ON t1.a = t2.a AND t2.b < 2
            |""".stripMargin)

        checkInSubqueryPredicate(df, false, true)

        checkAnswer(df, Row(Array(0), 1) :: Row(Array(1), 1) :: Nil)
      }
    }
  }

}

class ShufflePruningSuiteAEOff extends ShufflePruningSuiteBase {
  override val adaptiveExecutionOn: Boolean = false
}

class ShufflePruningSuiteAEOn extends ShufflePruningSuiteBase {
  override val adaptiveExecutionOn: Boolean = true
}
