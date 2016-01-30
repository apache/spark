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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.{Exchange, WholeStageCodegen}
import org.apache.spark.sql.execution.datasources.BucketSpec
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

  // To verify if pruning works, we compare the results before filtering
  private def checkPrunedAnswers(
      sourceDataFrame: DataFrame,
      filterCondition: Column,
      expectedAnswer: DataFrame): Unit = {
    val filter = sourceDataFrame.filter(filterCondition).queryExecution.executedPlan
    assert(
      filter.isInstanceOf[execution.Filter] ||
      (filter.isInstanceOf[WholeStageCodegen] &&
        filter.asInstanceOf[WholeStageCodegen].plan.isInstanceOf[execution.Filter]))
    checkAnswer(
      expectedAnswer.orderBy(expectedAnswer.logicalPlan.output.map(attr => Column(attr)) : _*),
      filter.children.head.executeCollectPublic().sortBy(_.toString()))
  }

  test("read partitioning bucketed tables with bucket pruning filters") {
    val df = (10 until 50).map(i => (i % 5, i % 13 + 10, i.toString)).toDF("i", "j", "k")

    withTable("bucketed_table") {
      // The number of buckets should be large enough to make sure each bucket contains
      // at most one bucketing key value.
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .partitionBy("i")
        .bucketBy(50, "j")
        .saveAsTable("bucketed_table")
      for (j <- 10 until 23) {
        // Case 1: EqualTo
        checkPrunedAnswers(
          hiveContext.table("bucketed_table").select("i", "j", "k"),
          filterCondition = $"j" === j,
          df.select("i", "j", "k").filter($"j" === j))

        // Case 2: EqualNullSafe
        checkPrunedAnswers(
          hiveContext.table("bucketed_table").select("i", "j", "k"),
          filterCondition = $"j" <=> j,
          df.select("i", "j", "k").filter($"j" <=> j))

        // Case 3: In
        checkPrunedAnswers(
          hiveContext.table("bucketed_table").select("i", "j", "k"),
          filterCondition = $"j".isin(j, j + 1, j + 2, j + 3),
          df.select("i", "j", "k").filter($"j".isin(j, j + 1, j + 2, j + 3)))
      }
      // Case 4: InSet
      // The buckets that all these inset elements are counted. Thus, the returned results will
      // contain many ineligible rows. Below, we just check if the returned results are right.
      // If buckets are incorrectly pruned, the return results could miss the eligible rows.
      checkAnswer(
        df.select("i", "j", "k").filter($"j".isin(10, 11, 12, 13, -3, -4, -5, -6, -7, -8, -9))
          .sort("i", "j", "k"),
        hiveContext.table("bucketed_table").select("i", "j", "k")
          .filter($"j".isin(10, 11, 12, 13, -3, -4, -5, -6, -7, -8, -9)).sort("i", "j", "k"))
    }
  }

  test("read non-partitioning bucketed tables with bucket pruning filters") {
    val df = (10 until 50).map(i => (i % 5, i % 13 + 10, i.toString)).toDF("i", "j", "k")

    withTable("bucketed_table") {
      // The number of buckets should be large enough to make sure each bucket contains
      // at most one bucketing key value.
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .bucketBy(50, "j")
        .saveAsTable("bucketed_table")

      for (j <- 10 until 23) {
        checkPrunedAnswers(
          hiveContext.table("bucketed_table").select("i", "j", "k"),
          filterCondition = $"j" === j,
          df.select("i", "j", "k").filter($"j" === j))
      }
    }
  }

  test("read partitioning bucketed tables having null in bucketing key") {
    withTable("bucketed_table") {
      // The number of buckets should be large enough to make sure each bucket contains
      // at most one bucketing key value.
      // json does not support predicate push-down, and thus json is used here
      nullStrings.write
        .format("json")
        .partitionBy("n")
        .bucketBy(50, "s")
        .saveAsTable("bucketed_table")

      // Case 1: isNull
      checkPrunedAnswers(
        hiveContext.table("bucketed_table").select("n", "s"),
        filterCondition = $"s".isNull,
        nullStrings.select("n", "s").filter($"s".isNull))

      // Case 2: <=> null
      checkPrunedAnswers(
        hiveContext.table("bucketed_table").select("n", "s"),
        filterCondition = $"s" <=> null,
        nullStrings.select("n", "s").filter($"s" <=> null))
    }
  }

  test("read partitioning bucketed tables having composite filters") {
    val df = (10 until 50).map(i => (i % 5, i % 13 + 10, i)).toDF("i", "j", "k")

    withTable("bucketed_table") {
      // The number of buckets should be large enough to make sure each bucket contains
      // at most one bucketing key value.
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .partitionBy("i")
        .bucketBy(50, "j")
        .saveAsTable("bucketed_table")

      for (j <- 10 until 23) {
        checkPrunedAnswers(
          hiveContext.table("bucketed_table").select("i", "j", "k"),
          filterCondition = $"j" === j && $"k" > $"j",
          // the filter $"k" > $"j" is not applied
          df.select("i", "j", "k").filter($"j" === j))

        checkPrunedAnswers(
          hiveContext.table("bucketed_table").select("i", "j", "k"),
          filterCondition = $"j" === j && $"i" > j % 5,
          // $"i" > j % 5 is partitioning only filter.
          // $"j" === j is bucketing only filter and each bucket corresponds to at most one value.
          // Thus, the final results apply both conditions
          df.select("i", "j", "k").filter($"j" === j && $"i" > j % 5))
      }
    }
  }

  private val df1 = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k").as("df1")
  private val df2 = (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k").as("df2")

  /**
   * A helper method to test the bucket read functionality using join.  It will save `df1` and `df2`
   * to hive tables, bucketed or not, according to the given bucket specifics.  Next we will join
   * these 2 tables, and firstly make sure the answer is corrected, and then check if the shuffle
   * exists as user expected according to the `shuffleLeft` and `shuffleRight`.
   */
  private def testBucketing(
      bucketSpecLeft: Option[BucketSpec],
      bucketSpecRight: Option[BucketSpec],
      joinColumns: Seq[String],
      shuffleLeft: Boolean,
      shuffleRight: Boolean): Unit = {
    withTable("bucketed_table1", "bucketed_table2") {
      def withBucket(writer: DataFrameWriter, bucketSpec: Option[BucketSpec]): DataFrameWriter = {
        bucketSpec.map { spec =>
          writer.bucketBy(
            spec.numBuckets,
            spec.bucketColumnNames.head,
            spec.bucketColumnNames.tail: _*)
        }.getOrElse(writer)
      }

      withBucket(df1.write.format("parquet"), bucketSpecLeft).saveAsTable("bucketed_table1")
      withBucket(df2.write.format("parquet"), bucketSpecRight).saveAsTable("bucketed_table2")

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
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    testBucketing(bucketSpec, bucketSpec, Seq("i", "j"), shuffleLeft = false, shuffleRight = false)
  }

  // Enable it after fix https://issues.apache.org/jira/browse/SPARK-12704
  ignore("avoid shuffle when join keys are a super-set of bucket keys") {
    val bucketSpec = Some(BucketSpec(8, Seq("i"), Nil))
    testBucketing(bucketSpec, bucketSpec, Seq("i", "j"), shuffleLeft = false, shuffleRight = false)
  }

  test("only shuffle one side when join bucketed table and non-bucketed table") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    testBucketing(bucketSpec, None, Seq("i", "j"), shuffleLeft = false, shuffleRight = true)
  }

  test("only shuffle one side when 2 bucketed tables have different bucket number") {
    val bucketSpec1 = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketSpec2 = Some(BucketSpec(5, Seq("i", "j"), Nil))
    testBucketing(bucketSpec1, bucketSpec2, Seq("i", "j"), shuffleLeft = false, shuffleRight = true)
  }

  test("only shuffle one side when 2 bucketed tables have different bucket keys") {
    val bucketSpec1 = Some(BucketSpec(8, Seq("i"), Nil))
    val bucketSpec2 = Some(BucketSpec(8, Seq("j"), Nil))
    testBucketing(bucketSpec1, bucketSpec2, Seq("i"), shuffleLeft = false, shuffleRight = true)
  }

  test("shuffle when join keys are not equal to bucket keys") {
    val bucketSpec = Some(BucketSpec(8, Seq("i"), Nil))
    testBucketing(bucketSpec, bucketSpec, Seq("j"), shuffleLeft = true, shuffleRight = true)
  }

  test("shuffle when join 2 bucketed tables with bucketing disabled") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    withSQLConf(SQLConf.BUCKETING_ENABLED.key -> "false") {
      testBucketing(bucketSpec, bucketSpec, Seq("i", "j"), shuffleLeft = true, shuffleRight = true)
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
