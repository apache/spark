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

import java.{lang => jl}
import java.sql.{Date, Timestamp}

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.test.SQLTestData.ArrayData
import org.apache.spark.sql.types._


/**
 * End-to-end suite testing statistics collection and use on both entire table and columns.
 */
class StatisticsCollectionSuite extends StatisticsCollectionTestBase with SharedSQLContext {
  import testImplicits._

  private def checkTableStats(tableName: String, expectedRowCount: Option[Int])
    : Option[Statistics] = {
    val df = spark.table(tableName)
    val stats = df.queryExecution.analyzed.collect { case rel: LogicalRelation =>
      assert(rel.catalogTable.get.stats.flatMap(_.rowCount) === expectedRowCount)
      rel.catalogTable.get.stats
    }
    assert(stats.size == 1)
    stats.head
  }

  test("estimates the size of a limit 0 on outer join") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      val df1 = spark.table("test")
      val df2 = spark.table("test").limit(0)
      val df = df1.join(df2, Seq("k"), "left")

      val sizes = df.queryExecution.analyzed.collect { case g: Join =>
        g.statistics.sizeInBytes
      }

      assert(sizes.size === 1, s"number of Join nodes is wrong:\n ${df.queryExecution}")
      assert(sizes.head === BigInt(96),
        s"expected exact size 96 for table 'test', got: ${sizes.head}")
    }
  }

  test("analyze column command - unsupported types and invalid columns") {
    val tableName = "column_stats_test1"
    withTable(tableName) {
      Seq(ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3)))).toDF().write.saveAsTable(tableName)

      // Test unsupported data types
      val err1 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS data")
      }
      assert(err1.message.contains("does not support statistics collection"))

      // Test invalid columns
      val err2 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS some_random_column")
      }
      assert(err2.message.contains("does not exist"))
    }
  }

  test("test table-level statistics for data source table") {
    val tableName = "tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i INT, j STRING) USING parquet")
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto(tableName)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
      checkTableStats(tableName, expectedRowCount = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      checkTableStats(tableName, expectedRowCount = Some(2))
    }
  }

  test("SPARK-15392: DataFrame created from RDD should not be broadcasted") {
    val rdd = sparkContext.range(1, 100).map(i => Row(i, i))
    val df = spark.createDataFrame(rdd, new StructType().add("a", LongType).add("b", LongType))
    assert(df.queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
    assert(df.selectExpr("a").queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
  }

  test("estimates the size of limit") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      Seq((0, 1), (1, 24), (2, 48)).foreach { case (limit, expected) =>
        val df = sql(s"""SELECT * FROM test limit $limit""")

        val sizesGlobalLimit = df.queryExecution.analyzed.collect { case g: GlobalLimit =>
          g.statistics.sizeInBytes
        }
        assert(sizesGlobalLimit.size === 1, s"Size wrong for:\n ${df.queryExecution}")
        assert(sizesGlobalLimit.head === BigInt(expected),
          s"expected exact size $expected for table 'test', got: ${sizesGlobalLimit.head}")

        val sizesLocalLimit = df.queryExecution.analyzed.collect { case l: LocalLimit =>
          l.statistics.sizeInBytes
        }
        assert(sizesLocalLimit.size === 1, s"Size wrong for:\n ${df.queryExecution}")
        assert(sizesLocalLimit.head === BigInt(expected),
          s"expected exact size $expected for table 'test', got: ${sizesLocalLimit.head}")
      }
    }
  }

  test("column stats round trip serialization") {
    // Make sure we serialize and then deserialize and we will get the result data
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    stats.zip(df.schema).foreach { case ((k, v), field) =>
      withClue(s"column $k with type ${field.dataType}") {
        val roundtrip = ColumnStat.fromMap("table_is_foo", field, v.toMap)
        assert(roundtrip == Some(v))
      }
    }
  }

  test("analyze column command - result verification") {
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    checkColStats(df, stats)
  }

  test("column stats collection for null columns") {
    val dataTypes: Seq[(DataType, Int)] = Seq(
      BooleanType, ByteType, ShortType, IntegerType, LongType,
      DoubleType, FloatType, DecimalType.SYSTEM_DEFAULT,
      StringType, BinaryType, DateType, TimestampType
    ).zipWithIndex

    val df = sql("select " + dataTypes.map { case (tpe, idx) =>
      s"cast(null as ${tpe.sql}) as col$idx"
    }.mkString(", "))

    val expectedColStats = dataTypes.map { case (tpe, idx) =>
      (s"col$idx", ColumnStat(0, None, None, 1, tpe.defaultSize.toLong, tpe.defaultSize.toLong))
    }
    checkColStats(df, mutable.LinkedHashMap(expectedColStats: _*))
  }
}


/**
 * The base for test cases that we want to include in both the hive module (for verifying behavior
 * when using the Hive external catalog) as well as in the sql/core module.
 */
abstract class StatisticsCollectionTestBase extends QueryTest with SQLTestUtils {
  import testImplicits._

  private val dec1 = new java.math.BigDecimal("1.000000000000000000")
  private val dec2 = new java.math.BigDecimal("8.000000000000000000")
  private val d1 = Date.valueOf("2016-05-08")
  private val d2 = Date.valueOf("2016-05-09")
  private val t1 = Timestamp.valueOf("2016-05-08 00:00:01")
  private val t2 = Timestamp.valueOf("2016-05-09 00:00:02")

  /**
   * Define a very simple 3 row table used for testing column serialization.
   * Note: last column is seq[int] which doesn't support stats collection.
   */
  protected val data = Seq[
    (jl.Boolean, jl.Byte, jl.Short, jl.Integer, jl.Long,
      jl.Double, jl.Float, java.math.BigDecimal,
      String, Array[Byte], Date, Timestamp,
      Seq[Int])](
    (false, 1.toByte, 1.toShort, 1, 1L, 1.0, 1.0f, dec1, "s1", "b1".getBytes, d1, t1, null),
    (true, 2.toByte, 3.toShort, 4, 5L, 6.0, 7.0f, dec2, "ss9", "bb0".getBytes, d2, t2, null),
    (null, null, null, null, null, null, null, null, null, null, null, null, null)
  )

  /** A mapping from column to the stats collected. */
  protected val stats = mutable.LinkedHashMap(
    "cbool" -> ColumnStat(2, Some(false), Some(true), 1, 1, 1),
    "cbyte" -> ColumnStat(2, Some(1L), Some(2L), 1, 1, 1),
    "cshort" -> ColumnStat(2, Some(1L), Some(3L), 1, 2, 2),
    "cint" -> ColumnStat(2, Some(1L), Some(4L), 1, 4, 4),
    "clong" -> ColumnStat(2, Some(1L), Some(5L), 1, 8, 8),
    "cdouble" -> ColumnStat(2, Some(1.0), Some(6.0), 1, 8, 8),
    "cfloat" -> ColumnStat(2, Some(1.0), Some(7.0), 1, 4, 4),
    "cdecimal" -> ColumnStat(2, Some(dec1), Some(dec2), 1, 16, 16),
    "cstring" -> ColumnStat(2, None, None, 1, 3, 3),
    "cbinary" -> ColumnStat(2, None, None, 1, 3, 3),
    "cdate" -> ColumnStat(2, Some(d1), Some(d2), 1, 4, 4),
    "ctimestamp" -> ColumnStat(2, Some(t1), Some(t2), 1, 8, 8)
  )

  private val randomName = new Random(31)

  /**
   * Compute column stats for the given DataFrame and compare it with colStats.
   */
  def checkColStats(
      df: DataFrame,
      colStats: mutable.LinkedHashMap[String, ColumnStat]): Unit = {
    val tableName = "column_stats_test_" + randomName.nextInt(1000)
    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Collect statistics
      sql(s"analyze table $tableName compute STATISTICS FOR COLUMNS " +
        colStats.keys.mkString(", "))

      // Validate statistics
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assert(table.stats.isDefined)
      assert(table.stats.get.colStats.size == colStats.size)

      colStats.foreach { case (k, v) =>
        withClue(s"column $k") {
          assert(table.stats.get.colStats(k) == v)
        }
      }
    }
  }

  // This test will be run twice: with and without Hive support
  test("SPARK-18856: non-empty partitioned table should not report zero size") {
    withTable("ds_tbl", "hive_tbl") {
      spark.range(100).select($"id", $"id" % 5 as "p").write.partitionBy("p").saveAsTable("ds_tbl")
      val stats = spark.table("ds_tbl").queryExecution.optimizedPlan.statistics
      assert(stats.sizeInBytes > 0, "non-empty partitioned table should not report zero size.")

      if (spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive") {
        sql("CREATE TABLE hive_tbl(i int) PARTITIONED BY (j int)")
        sql("INSERT INTO hive_tbl PARTITION(j=1) SELECT 1")
        val stats2 = spark.table("hive_tbl").queryExecution.optimizedPlan.statistics
        assert(stats2.sizeInBytes > 0, "non-empty partitioned table should not report zero size.")
      }
    }
  }
}
