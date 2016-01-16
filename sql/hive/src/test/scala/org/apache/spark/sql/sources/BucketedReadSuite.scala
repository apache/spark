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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, QueryTest, SQLConf}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.Exchange
import org.apache.spark.sql.execution.joins.SortMergeJoin
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class BucketedReadSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("read bucketed data") {
    val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("bucketed_table") {
      df.write
        .format("parquet")
        .partitionBy("i")
        .bucketBy(8, "j", "k")
        .saveAsTable("bucketed_table")

      for (i <- 0 until 5) {
        val rdd = hiveContext.table("bucketed_table").filter($"i" === i).queryExecution.toRdd
        assert(rdd.partitions.length == 8)

        val attrs = df.select("j", "k").schema.toAttributes
        val checkBucketId = rdd.mapPartitionsWithIndex((index, rows) => {
          val getBucketId = UnsafeProjection.create(
            HashPartitioning(attrs, 8).partitionIdExpression :: Nil,
            attrs)
          rows.map(row => getBucketId(row).getInt(0) == index)
        })

        assert(checkBucketId.collect().reduce(_ && _))
      }
    }
  }

  private val df1 = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k").as("df1")
  private val df2 = (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k").as("df2")

  private def testBucketing(
      bucketing1: DataFrameWriter => DataFrameWriter,
      bucketing2: DataFrameWriter => DataFrameWriter,
      joinColumns: Seq[String],
      shuffleLeft: Boolean,
      shuffleRight: Boolean): Unit = {
    withTable("bucketed_table1", "bucketed_table2") {
      bucketing1(df1.write.format("parquet")).saveAsTable("bucketed_table1")
      bucketing2(df2.write.format("parquet")).saveAsTable("bucketed_table2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        val t1 = hiveContext.table("bucketed_table1")
        val t2 = hiveContext.table("bucketed_table2")
        val joined = t1.join(t2, joinCondition(t1, t2, joinColumns))

        // First check the result is corrected.
        checkAnswer(
          joined.sort("bucketed_table1.k", "bucketed_table2.k"),
          df1.join(df2, joinCondition(df1, df2, joinColumns)).sort("df1.k", "df2.k"))

        assert(joined.queryExecution.executedPlan.isInstanceOf[SortMergeJoin])
        val joinOperator = joined.queryExecution.executedPlan.asInstanceOf[SortMergeJoin]

        assert(joinOperator.left.find(_.isInstanceOf[Exchange]).isDefined == shuffleLeft)
        assert(joinOperator.right.find(_.isInstanceOf[Exchange]).isDefined == shuffleRight)
      }
    }
  }

  private def joinCondition(left: DataFrame, right: DataFrame, joinCols: Seq[String]): Column = {
    joinCols.map(col => left(col) === right(col)).reduce(_ && _)
  }

  test("avoid shuffle when join 2 bucketed tables") {
    val bucketing = (writer: DataFrameWriter) => writer.bucketBy(8, "i", "j")
    testBucketing(bucketing, bucketing, Seq("i", "j"), shuffleLeft = false, shuffleRight = false)
  }

  // Enable it after fix https://issues.apache.org/jira/browse/SPARK-12704
  ignore("avoid shuffle when join keys are a super-set of bucket keys") {
    val bucketing = (writer: DataFrameWriter) => writer.bucketBy(8, "i")
    testBucketing(bucketing, bucketing, Seq("i", "j"), shuffleLeft = false, shuffleRight = false)
  }

  test("only shuffle one side when join bucketed table and non-bucketed table") {
    val bucketing = (writer: DataFrameWriter) => writer.bucketBy(8, "i", "j")
    testBucketing(bucketing, identity, Seq("i", "j"), shuffleLeft = false, shuffleRight = true)
  }

  test("only shuffle one side when 2 bucketed tables have different bucket number") {
    val bucketing1 = (writer: DataFrameWriter) => writer.bucketBy(8, "i", "j")
    val bucketing2 = (writer: DataFrameWriter) => writer.bucketBy(5, "i", "j")
    testBucketing(bucketing1, bucketing2, Seq("i", "j"), shuffleLeft = false, shuffleRight = true)
  }

  test("only shuffle one side when 2 bucketed tables have different bucket keys") {
    val bucketing1 = (writer: DataFrameWriter) => writer.bucketBy(8, "i")
    val bucketing2 = (writer: DataFrameWriter) => writer.bucketBy(8, "j")
    testBucketing(bucketing1, bucketing2, Seq("i"), shuffleLeft = false, shuffleRight = true)
  }

  test("shuffle when join keys are not equal to bucket keys") {
    val bucketing = (writer: DataFrameWriter) => writer.bucketBy(8, "i")
    testBucketing(bucketing, bucketing, Seq("j"), shuffleLeft = true, shuffleRight = true)
  }

  test("shuffle when join 2 bucketed tables with bucketing disabled") {
    val bucketing = (writer: DataFrameWriter) => writer.bucketBy(8, "i", "j")
    withSQLConf(SQLConf.BUCKETING_ENABLED.key -> "false") {
      testBucketing(bucketing, bucketing, Seq("i", "j"), shuffleLeft = true, shuffleRight = true)
    }
  }

  test("avoid shuffle when grouping keys are equal to bucket keys") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i", "j").saveAsTable("bucketed_table")
      val tbl = hiveContext.table("bucketed_table")
      val agged = tbl.groupBy("i", "j").agg(max("k"))

      checkAnswer(
        agged.sort("i", "j"),
        df1.groupBy("i", "j").agg(max("k")).sort("i", "j"))

      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[Exchange]).isEmpty)
    }
  }

  test("avoid shuffle when grouping keys are a super-set of bucket keys") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
      val tbl = hiveContext.table("bucketed_table")
      val agged = tbl.groupBy("i", "j").agg(max("k"))

      checkAnswer(
        agged.sort("i", "j"),
        df1.groupBy("i", "j").agg(max("k")).sort("i", "j"))

      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[Exchange]).isEmpty)
    }
  }

  test("fallback to non-bucketing mode if there exists any malformed bucket files") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
      val tableDir = new File(hiveContext.warehousePath, "bucketed_table")
      Utils.deleteRecursively(tableDir)
      df1.write.parquet(tableDir.getAbsolutePath)

      val agged = hiveContext.table("bucketed_table").groupBy("i").count()
      // make sure we fall back to non-bucketing mode and can't avoid shuffle
      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[Exchange]).isDefined)
      checkAnswer(agged.sort("i"), df1.groupBy("i").count().sort("i"))
    }
  }
}
