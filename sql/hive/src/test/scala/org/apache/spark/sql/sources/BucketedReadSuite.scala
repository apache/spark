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
import java.net.URI

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{DataSourceScanExec, SortExec}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

class BucketedReadSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  private val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
  private val nullDF = (for {
    i <- 0 to 50
    s <- Seq(null, "a", "b", "c", "d", "e", "f", null, "g")
  } yield (i % 5, s, i % 13)).toDF("i", "j", "k")

  test("read bucketed data") {
    withTable("bucketed_table") {
      df.write
        .format("parquet")
        .partitionBy("i")
        .bucketBy(8, "j", "k")
        .saveAsTable("bucketed_table")

      for (i <- 0 until 5) {
        val table = spark.table("bucketed_table").filter($"i" === i)
        val query = table.queryExecution
        val output = query.analyzed.output
        val rdd = query.toRdd

        assert(rdd.partitions.length == 8)

        val attrs = table.select("j", "k").queryExecution.analyzed.output
        val checkBucketId = rdd.mapPartitionsWithIndex((index, rows) => {
          val getBucketId = UnsafeProjection.create(
            HashPartitioning(attrs, 8).partitionIdExpression :: Nil,
            output)
          rows.map(row => getBucketId(row).getInt(0) -> index)
        })
        checkBucketId.collect().foreach(r => assert(r._1 == r._2))
      }
    }
  }

  // To verify if the bucket pruning works, this function checks two conditions:
  //   1) Check if the pruned buckets (before filtering) are empty.
  //   2) Verify the final result is the same as the expected one
  private def checkPrunedAnswers(
      bucketSpec: BucketSpec,
      bucketValues: Seq[Integer],
      filterCondition: Column,
      originalDataFrame: DataFrame): Unit = {
    // This test verifies parts of the plan. Disable whole stage codegen.
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val bucketedDataFrame = spark.table("bucketed_table").select("i", "j", "k")
      val BucketSpec(numBuckets, bucketColumnNames, _) = bucketSpec
      // Limit: bucket pruning only works when the bucket column has one and only one column
      assert(bucketColumnNames.length == 1)
      val bucketColumnIndex = bucketedDataFrame.schema.fieldIndex(bucketColumnNames.head)
      val bucketColumn = bucketedDataFrame.schema.toAttributes(bucketColumnIndex)
      val matchedBuckets = new BitSet(numBuckets)
      bucketValues.foreach { value =>
        matchedBuckets.set(DataSourceStrategy.getBucketId(bucketColumn, numBuckets, value))
      }

      // Filter could hide the bug in bucket pruning. Thus, skipping all the filters
      val plan = bucketedDataFrame.filter(filterCondition).queryExecution.executedPlan
      val rdd = plan.find(_.isInstanceOf[DataSourceScanExec])
      assert(rdd.isDefined, plan)

      val checkedResult = rdd.get.execute().mapPartitionsWithIndex { case (index, iter) =>
        if (matchedBuckets.get(index % numBuckets) && iter.nonEmpty) Iterator(index) else Iterator()
      }
      // TODO: These tests are not testing the right columns.
//      // checking if all the pruned buckets are empty
//      val invalidBuckets = checkedResult.collect().toList
//      if (invalidBuckets.nonEmpty) {
//        fail(s"Buckets $invalidBuckets should have been pruned from:\n$plan")
//      }

      checkAnswer(
        bucketedDataFrame.filter(filterCondition).orderBy("i", "j", "k"),
        originalDataFrame.filter(filterCondition).orderBy("i", "j", "k"))
    }
  }

  test("read partitioning bucketed tables with bucket pruning filters") {
    withTable("bucketed_table") {
      val numBuckets = 8
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .partitionBy("i")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      for (j <- 0 until 13) {
        // Case 1: EqualTo
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = j :: Nil,
          filterCondition = $"j" === j,
          df)

        // Case 2: EqualNullSafe
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = j :: Nil,
          filterCondition = $"j" <=> j,
          df)

        // Case 3: In
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = Seq(j, j + 1, j + 2, j + 3),
          filterCondition = $"j".isin(j, j + 1, j + 2, j + 3),
          df)
      }
    }
  }

  test("read non-partitioning bucketed tables with bucket pruning filters") {
    withTable("bucketed_table") {
      val numBuckets = 8
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      for (j <- 0 until 13) {
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = j :: Nil,
          filterCondition = $"j" === j,
          df)
      }
    }
  }

  test("read partitioning bucketed tables having null in bucketing key") {
    withTable("bucketed_table") {
      val numBuckets = 8
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      // json does not support predicate push-down, and thus json is used here
      nullDF.write
        .format("json")
        .partitionBy("i")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      // Case 1: isNull
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = null :: Nil,
        filterCondition = $"j".isNull,
        nullDF)

      // Case 2: <=> null
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = null :: Nil,
        filterCondition = $"j" <=> null,
        nullDF)
    }
  }

  test("read partitioning bucketed tables having composite filters") {
    withTable("bucketed_table") {
      val numBuckets = 8
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .partitionBy("i")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      for (j <- 0 until 13) {
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = j :: Nil,
          filterCondition = $"j" === j && $"k" > $"j",
          df)

        checkPrunedAnswers(
          bucketSpec,
          bucketValues = j :: Nil,
          filterCondition = $"j" === j && $"i" > j % 5,
          df)
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
      shuffleRight: Boolean,
      sortLeft: Boolean = true,
      sortRight: Boolean = true): Unit = {
    withTable("bucketed_table1", "bucketed_table2") {
      def withBucket(
          writer: DataFrameWriter[Row],
          bucketSpec: Option[BucketSpec]): DataFrameWriter[Row] = {
        bucketSpec.map { spec =>
          writer.bucketBy(
            spec.numBuckets,
            spec.bucketColumnNames.head,
            spec.bucketColumnNames.tail: _*)

          if (spec.sortColumnNames.nonEmpty) {
            writer.sortBy(
              spec.sortColumnNames.head,
              spec.sortColumnNames.tail: _*
            )
          } else {
            writer
          }
        }.getOrElse(writer)
      }

      withBucket(df1.write.format("parquet"), bucketSpecLeft).saveAsTable("bucketed_table1")
      withBucket(df2.write.format("parquet"), bucketSpecRight).saveAsTable("bucketed_table2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val t1 = spark.table("bucketed_table1")
        val t2 = spark.table("bucketed_table2")
        val joined = t1.join(t2, joinCondition(t1, t2, joinColumns))

        // First check the result is corrected.
        checkAnswer(
          joined.sort("bucketed_table1.k", "bucketed_table2.k"),
          df1.join(df2, joinCondition(df1, df2, joinColumns)).sort("df1.k", "df2.k"))

        assert(joined.queryExecution.executedPlan.isInstanceOf[SortMergeJoinExec])
        val joinOperator = joined.queryExecution.executedPlan.asInstanceOf[SortMergeJoinExec]

        // check existence of shuffle
        assert(
          joinOperator.left.find(_.isInstanceOf[ShuffleExchange]).isDefined == shuffleLeft,
          s"expected shuffle in plan to be $shuffleLeft but found\n${joinOperator.left}")
        assert(
          joinOperator.right.find(_.isInstanceOf[ShuffleExchange]).isDefined == shuffleRight,
          s"expected shuffle in plan to be $shuffleRight but found\n${joinOperator.right}")

        // check existence of sort
        assert(
          joinOperator.left.find(_.isInstanceOf[SortExec]).isDefined == sortLeft,
          s"expected sort in plan to be $shuffleLeft but found\n${joinOperator.left}")
        assert(
          joinOperator.right.find(_.isInstanceOf[SortExec]).isDefined == sortRight,
          s"expected sort in plan to be $shuffleRight but found\n${joinOperator.right}")
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

  test("avoid shuffle and sort when bucket and sort columns are join keys") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j")))
    testBucketing(
      bucketSpec, bucketSpec, Seq("i", "j"),
      shuffleLeft = false, shuffleRight = false,
      sortLeft = false, sortRight = false
    )
  }

  test("avoid shuffle and sort when sort columns are a super set of join keys") {
    val bucketSpec1 = Some(BucketSpec(8, Seq("i"), Seq("i", "j")))
    val bucketSpec2 = Some(BucketSpec(8, Seq("i"), Seq("i", "k")))
    testBucketing(
      bucketSpec1, bucketSpec2, Seq("i"),
      shuffleLeft = false, shuffleRight = false,
      sortLeft = false, sortRight = false
    )
  }

  test("only sort one side when sort columns are different") {
    val bucketSpec1 = Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j")))
    val bucketSpec2 = Some(BucketSpec(8, Seq("i", "j"), Seq("k")))
    testBucketing(
      bucketSpec1, bucketSpec2, Seq("i", "j"),
      shuffleLeft = false, shuffleRight = false,
      sortLeft = false, sortRight = true
    )
  }

  test("only sort one side when sort columns are same but their ordering is different") {
    val bucketSpec1 = Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j")))
    val bucketSpec2 = Some(BucketSpec(8, Seq("i", "j"), Seq("j", "i")))
    testBucketing(
      bucketSpec1, bucketSpec2, Seq("i", "j"),
      shuffleLeft = false, shuffleRight = false,
      sortLeft = false, sortRight = true
    )
  }

  test("avoid shuffle when grouping keys are equal to bucket keys") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i", "j").saveAsTable("bucketed_table")
      val tbl = spark.table("bucketed_table")
      val agged = tbl.groupBy("i", "j").agg(max("k"))

      checkAnswer(
        agged.sort("i", "j"),
        df1.groupBy("i", "j").agg(max("k")).sort("i", "j"))

      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchange]).isEmpty)
    }
  }

  test("avoid shuffle when grouping keys are a super-set of bucket keys") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
      val tbl = spark.table("bucketed_table")
      val agged = tbl.groupBy("i", "j").agg(max("k"))

      checkAnswer(
        agged.sort("i", "j"),
        df1.groupBy("i", "j").agg(max("k")).sort("i", "j"))

      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchange]).isEmpty)
    }
  }

  test("error if there exists any malformed bucket files") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
      val warehouseFilePath = new URI(hiveContext.sparkSession.getWarehousePath).getPath
      val tableDir = new File(warehouseFilePath, "bucketed_table")
      Utils.deleteRecursively(tableDir)
      df1.write.parquet(tableDir.getAbsolutePath)

      val agged = spark.table("bucketed_table").groupBy("i").count()
      val error = intercept[Exception] {
        agged.count()
      }

      assert(error.getCause().toString contains "Invalid bucket file")
    }
  }

  test("disable bucketing when the output doesn't contain all bucketing columns") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")

      checkAnswer(hiveContext.table("bucketed_table").select("j"), df1.select("j"))

      checkAnswer(hiveContext.table("bucketed_table").groupBy("j").agg(max("k")),
        df1.groupBy("j").agg(max("k")))
    }
  }
}
