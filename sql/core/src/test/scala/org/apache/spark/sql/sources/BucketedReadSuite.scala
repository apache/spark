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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{DataSourceScanExec, FileSourceScanExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

class BucketedReadWithoutHiveSupportSuite extends BucketedReadSuite with SharedSQLContext {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    assert(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
  }
}


abstract class BucketedReadSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  private lazy val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
  private lazy val nullDF = (for {
    i <- 0 to 50
    s <- Seq(null, "a", "b", "c", "d", "e", "f", null, "g")
  } yield (i % 5, s, i % 13)).toDF("i", "j", "k")

  // number of buckets that doesn't yield empty buckets when bucketing on column j on df/nullDF
  // empty buckets before filtering might hide bugs in pruning logic
  private val NumBucketsForPruningDF = 7
  private val NumBucketsForPruningNullDf = 5

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

  private def getFileScan(plan: SparkPlan): FileSourceScanExec = {
    val fileScan = plan.collect { case f: FileSourceScanExec => f }
    assert(fileScan.nonEmpty, plan)
    fileScan.head
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

      // Filter could hide the bug in bucket pruning. Thus, skipping all the filters
      val plan = bucketedDataFrame.filter(filterCondition).queryExecution.executedPlan
      val fileScan = getFileScan(plan)

      // if nothing should be pruned, skip the pruning test
      if (bucketValues.nonEmpty) {
        val matchedBuckets = new BitSet(numBuckets)
        bucketValues.foreach { value =>
          matchedBuckets.set(BucketingUtils.getBucketIdFromValue(bucketColumn, numBuckets, value))
        }
        val invalidBuckets = fileScan.execute().mapPartitionsWithIndex { case (index, iter) =>
          // return indexes of partitions that should have been pruned and are not empty
          if (!matchedBuckets.get(index % numBuckets) && iter.nonEmpty) {
            Iterator(index)
          } else {
            Iterator()
          }
        }.collect()

        if (invalidBuckets.nonEmpty) {
          fail(s"Buckets ${invalidBuckets.mkString(",")} should have been pruned from:\n$plan")
        }
      }

      checkAnswer(
        bucketedDataFrame.filter(filterCondition).orderBy("i", "j", "k"),
        originalDataFrame.filter(filterCondition).orderBy("i", "j", "k"))
    }
  }

  test("read partitioning bucketed tables with bucket pruning filters") {
    withTable("bucketed_table") {
      val numBuckets = NumBucketsForPruningDF
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

        // Case 4: InSet
        val inSetExpr = expressions.InSet($"j".expr, Set(j, j + 1, j + 2, j + 3))
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = Seq(j, j + 1, j + 2, j + 3),
          filterCondition = Column(inSetExpr),
          df)
      }
    }
  }

  test("read non-partitioning bucketed tables with bucket pruning filters") {
    withTable("bucketed_table") {
      val numBuckets = NumBucketsForPruningDF
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
      val numBuckets = NumBucketsForPruningNullDf
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
      val numBuckets = NumBucketsForPruningDF
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

        // check multiple bucket values OR condition
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = Seq(j, j + 1),
          filterCondition = $"j" === j || $"j" === (j + 1),
          df)

        // check bucket value and none bucket value OR condition
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = Nil,
          filterCondition = $"j" === j || $"i" === 0,
          df)

        // check AND condition in complex expression
        checkPrunedAnswers(
          bucketSpec,
          bucketValues = Seq(j),
          filterCondition = ($"i" === 0 || $"k" > $"j") && $"j" === j,
          df)
      }
    }
  }

  test("read bucketed table without filters") {
    withTable("bucketed_table") {
      val numBuckets = NumBucketsForPruningDF
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      // json does not support predicate push-down, and thus json is used here
      df.write
        .format("json")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      val bucketedDataFrame = spark.table("bucketed_table").select("i", "j", "k")
      val plan = bucketedDataFrame.queryExecution.executedPlan
      val fileScan = getFileScan(plan)

      val emptyBuckets = fileScan.execute().mapPartitionsWithIndex { case (index, iter) =>
        // return indexes of empty partitions
        if (iter.isEmpty) {
          Iterator(index)
        } else {
          Iterator()
        }
      }.collect()

      if (emptyBuckets.nonEmpty) {
        fail(s"Buckets ${emptyBuckets.mkString(",")} should not have been pruned from:\n$plan")
      }

      checkAnswer(
        bucketedDataFrame.orderBy("i", "j", "k"),
        df.orderBy("i", "j", "k"))
    }
  }

  private lazy val df1 =
    (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k").as("df1")
  private lazy val df2 =
    (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k").as("df2")

  case class BucketedTableTestSpec(
      bucketSpec: Option[BucketSpec],
      numPartitions: Int = 10,
      expectedShuffle: Boolean = true,
      expectedSort: Boolean = true)

  /**
   * A helper method to test the bucket read functionality using join.  It will save `df1` and `df2`
   * to hive tables, bucketed or not, according to the given bucket specifics.  Next we will join
   * these 2 tables, and firstly make sure the answer is corrected, and then check if the shuffle
   * exists as user expected according to the `shuffleLeft` and `shuffleRight`.
   */
  private def testBucketing(
      bucketedTableTestSpecLeft: BucketedTableTestSpec,
      bucketedTableTestSpecRight: BucketedTableTestSpec,
      joinType: String = "inner",
      joinCondition: (DataFrame, DataFrame) => Column): Unit = {
    val BucketedTableTestSpec(bucketSpecLeft, numPartitionsLeft, shuffleLeft, sortLeft) =
      bucketedTableTestSpecLeft
    val BucketedTableTestSpec(bucketSpecRight, numPartitionsRight, shuffleRight, sortRight) =
      bucketedTableTestSpecRight

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

      withBucket(df1.repartition(numPartitionsLeft).write.format("parquet"), bucketSpecLeft)
        .saveAsTable("bucketed_table1")
      withBucket(df2.repartition(numPartitionsRight).write.format("parquet"), bucketSpecRight)
        .saveAsTable("bucketed_table2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val t1 = spark.table("bucketed_table1")
        val t2 = spark.table("bucketed_table2")
        val joined = t1.join(t2, joinCondition(t1, t2), joinType)

        // First check the result is corrected.
        checkAnswer(
          joined.sort("bucketed_table1.k", "bucketed_table2.k"),
          df1.join(df2, joinCondition(df1, df2), joinType).sort("df1.k", "df2.k"))

        assert(joined.queryExecution.executedPlan.isInstanceOf[SortMergeJoinExec])
        val joinOperator = joined.queryExecution.executedPlan.asInstanceOf[SortMergeJoinExec]

        // check existence of shuffle
        assert(
          joinOperator.left.find(_.isInstanceOf[ShuffleExchangeExec]).isDefined == shuffleLeft,
          s"expected shuffle in plan to be $shuffleLeft but found\n${joinOperator.left}")
        assert(
          joinOperator.right.find(_.isInstanceOf[ShuffleExchangeExec]).isDefined == shuffleRight,
          s"expected shuffle in plan to be $shuffleRight but found\n${joinOperator.right}")

        // check existence of sort
        assert(
          joinOperator.left.find(_.isInstanceOf[SortExec]).isDefined == sortLeft,
          s"expected sort in the left child to be $sortLeft but found\n${joinOperator.left}")
        assert(
          joinOperator.right.find(_.isInstanceOf[SortExec]).isDefined == sortRight,
          s"expected sort in the right child to be $sortRight but found\n${joinOperator.right}")
      }
    }
  }

  private def joinCondition(joinCols: Seq[String]) (left: DataFrame, right: DataFrame): Column = {
    joinCols.map(col => left(col) === right(col)).reduce(_ && _)
  }

  test("avoid shuffle when join 2 bucketed tables") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j"))
    )
  }

  // Enable it after fix https://issues.apache.org/jira/browse/SPARK-12704
  ignore("avoid shuffle when join keys are a super-set of bucket keys") {
    val bucketSpec = Some(BucketSpec(8, Seq("i"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j"))
    )
  }

  test("only shuffle one side when join bucketed table and non-bucketed table") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(None, expectedShuffle = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j"))
    )
  }

  test("only shuffle one side when 2 bucketed tables have different bucket number") {
    val bucketSpecLeft = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketSpecRight = Some(BucketSpec(5, Seq("i", "j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpecLeft, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpecRight, expectedShuffle = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j"))
    )
  }

  test("only shuffle one side when 2 bucketed tables have different bucket keys") {
    val bucketSpecLeft = Some(BucketSpec(8, Seq("i"), Nil))
    val bucketSpecRight = Some(BucketSpec(8, Seq("j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpecLeft, expectedShuffle = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpecRight, expectedShuffle = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i"))
    )
  }

  test("shuffle when join keys are not equal to bucket keys") {
    val bucketSpec = Some(BucketSpec(8, Seq("i"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = true)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpec, expectedShuffle = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("j"))
    )
  }

  test("shuffle when join 2 bucketed tables with bucketing disabled") {
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Nil))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = true)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(bucketSpec, expectedShuffle = true)
    withSQLConf(SQLConf.BUCKETING_ENABLED.key -> "false") {
      testBucketing(
        bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
        bucketedTableTestSpecRight = bucketedTableTestSpecRight,
        joinCondition = joinCondition(Seq("i", "j"))
      )
    }
  }

  test("check sort and shuffle when bucket and sort columns are join keys") {
    // In case of bucketing, its possible to have multiple files belonging to the
    // same bucket in a given relation. Each of these files are locally sorted
    // but those files combined together are not globally sorted. Given that,
    // the RDD partition will not be sorted even if the relation has sort columns set
    // Therefore, we still need to keep the Sort in both sides.
    val bucketSpec = Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j")))

    val bucketedTableTestSpecLeft1 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 50, expectedShuffle = false, expectedSort = true)
    val bucketedTableTestSpecRight1 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft1,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight1,
      joinCondition = joinCondition(Seq("i", "j"))
    )

    val bucketedTableTestSpecLeft2 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    val bucketedTableTestSpecRight2 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 50, expectedShuffle = false, expectedSort = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft2,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight2,
      joinCondition = joinCondition(Seq("i", "j"))
    )

    val bucketedTableTestSpecLeft3 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 50, expectedShuffle = false, expectedSort = true)
    val bucketedTableTestSpecRight3 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 50, expectedShuffle = false, expectedSort = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft3,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight3,
      joinCondition = joinCondition(Seq("i", "j"))
    )

    val bucketedTableTestSpecLeft4 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    val bucketedTableTestSpecRight4 = BucketedTableTestSpec(
      bucketSpec, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft4,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight4,
      joinCondition = joinCondition(Seq("i", "j"))
    )
  }

  test("avoid shuffle and sort when sort columns are a super set of join keys") {
    val bucketSpecLeft = Some(BucketSpec(8, Seq("i"), Seq("i", "j")))
    val bucketSpecRight = Some(BucketSpec(8, Seq("i"), Seq("i", "k")))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(
      bucketSpecLeft, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(
      bucketSpecRight, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i"))
    )
  }

  test("only sort one side when sort columns are different") {
    val bucketSpecLeft = Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j")))
    val bucketSpecRight = Some(BucketSpec(8, Seq("i", "j"), Seq("k")))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(
      bucketSpecLeft, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(
      bucketSpecRight, numPartitions = 1, expectedShuffle = false, expectedSort = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j"))
    )
  }

  test("only sort one side when sort columns are same but their ordering is different") {
    val bucketSpecLeft = Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j")))
    val bucketSpecRight = Some(BucketSpec(8, Seq("i", "j"), Seq("j", "i")))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(
      bucketSpecLeft, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(
      bucketSpecRight, numPartitions = 1, expectedShuffle = false, expectedSort = true)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinCondition = joinCondition(Seq("i", "j"))
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

      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)
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

      assert(agged.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)
    }
  }

  test("SPARK-17698 Join predicates should not contain filter clauses") {
    val bucketSpec = Some(BucketSpec(8, Seq("i"), Seq("i")))
    val bucketedTableTestSpecLeft = BucketedTableTestSpec(
      bucketSpec, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    val bucketedTableTestSpecRight = BucketedTableTestSpec(
      bucketSpec, numPartitions = 1, expectedShuffle = false, expectedSort = false)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
      bucketedTableTestSpecRight = bucketedTableTestSpecRight,
      joinType = "fullouter",
      joinCondition = (left: DataFrame, right: DataFrame) => {
        val joinPredicates = Seq("i").map(col => left(col) === right(col)).reduce(_ && _)
        val filterLeft = left("i") === Literal("1")
        val filterRight = right("i") === Literal("1")
        joinPredicates && filterLeft && filterRight
      }
    )
  }

  test("SPARK-19122 Re-order join predicates if they match with the child's output partitioning") {
    val bucketedTableTestSpec = BucketedTableTestSpec(
      Some(BucketSpec(8, Seq("i", "j", "k"), Seq("i", "j", "k"))),
      numPartitions = 1,
      expectedShuffle = false,
      expectedSort = false)

    // If the set of join columns is equal to the set of bucketed + sort columns, then
    // the order of join keys in the query should not matter and there should not be any shuffle
    // and sort added in the query plan
    Seq(
      Seq("i", "j", "k"),
      Seq("i", "k", "j"),
      Seq("j", "k", "i"),
      Seq("j", "i", "k"),
      Seq("k", "j", "i"),
      Seq("k", "i", "j")
    ).foreach(joinKeys => {
      testBucketing(
        bucketedTableTestSpecLeft = bucketedTableTestSpec,
        bucketedTableTestSpecRight = bucketedTableTestSpec,
        joinCondition = joinCondition(joinKeys)
      )
    })
  }

  test("SPARK-19122 No re-ordering should happen if set of join columns != set of child's " +
    "partitioning columns") {

    // join predicates is a super set of child's partitioning columns
    val bucketedTableTestSpec1 =
      BucketedTableTestSpec(Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))), numPartitions = 1)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpec1,
      bucketedTableTestSpecRight = bucketedTableTestSpec1,
      joinCondition = joinCondition(Seq("i", "j", "k"))
    )

    // child's partitioning columns is a super set of join predicates
    val bucketedTableTestSpec2 =
      BucketedTableTestSpec(Some(BucketSpec(8, Seq("i", "j", "k"), Seq("i", "j", "k"))),
        numPartitions = 1)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpec2,
      bucketedTableTestSpecRight = bucketedTableTestSpec2,
      joinCondition = joinCondition(Seq("i", "j"))
    )

    // set of child's partitioning columns != set join predicates (despite the lengths of the
    // sets are same)
    val bucketedTableTestSpec3 =
      BucketedTableTestSpec(Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))), numPartitions = 1)
    testBucketing(
      bucketedTableTestSpecLeft = bucketedTableTestSpec3,
      bucketedTableTestSpecRight = bucketedTableTestSpec3,
      joinCondition = joinCondition(Seq("j", "k"))
    )
  }

  test("SPARK-22042 ReorderJoinPredicates can break when child's partitioning is not decided") {
    withTable("bucketed_table", "table1", "table2") {
      df.write.format("parquet").saveAsTable("table1")
      df.write.format("parquet").saveAsTable("table2")
      df.write.format("parquet").bucketBy(8, "j", "k").saveAsTable("bucketed_table")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        checkAnswer(
          sql("""
                |SELECT ab.i, ab.j, ab.k, c.i, c.j, c.k
                |FROM (
                |  SELECT a.i, a.j, a.k
                |  FROM bucketed_table a
                |  JOIN table1 b
                |  ON a.i = b.i
                |) ab
                |JOIN table2 c
                |ON ab.i = c.i
              """.stripMargin),
          sql("""
                |SELECT a.i, a.j, a.k, c.i, c.j, c.k
                |FROM bucketed_table a
                |JOIN table1 b
                |ON a.i = b.i
                |JOIN table2 c
                |ON a.i = c.i
              """.stripMargin))
      }
    }
  }

  test("error if there exists any malformed bucket files") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
      val warehouseFilePath = new URI(spark.sessionState.conf.warehousePath).getPath
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

      val scanDF = spark.table("bucketed_table").select("j")
      assert(!getFileScan(scanDF.queryExecution.executedPlan).bucketedScan)
      checkAnswer(scanDF, df1.select("j"))

      val aggDF = spark.table("bucketed_table").groupBy("j").agg(max("k"))
      assert(!getFileScan(aggDF.queryExecution.executedPlan).bucketedScan)
      checkAnswer(aggDF, df1.groupBy("j").agg(max("k")))
    }
  }

  //  A test with a partition where the number of files in the partition is
  //  large. tests for the condition where the serialization of such a task may result in a stack
  //  overflow if the files list is stored in a recursive data structure
  //  This test is ignored because it takes long to run (~3 min)
  ignore("SPARK-27100 stack overflow: read data with large partitions") {
    val nCount = 20000
    // reshuffle data so that many small files are created
    val nShufflePartitions = 10000
    // and with one table partition, should result in 10000 files in one partition
    val nPartitions = 1
    val nBuckets = 2
    val dfPartitioned = (0 until nCount)
      .map(i => (i % nPartitions, i % nBuckets, i.toString)).toDF("i", "j", "k")

    // non-bucketed tables. This part succeeds without the fix for SPARK-27100
    try {
      withTable("non_bucketed_table") {
        dfPartitioned.repartition(nShufflePartitions)
          .write
          .format("parquet")
          .partitionBy("i")
          .saveAsTable("non_bucketed_table")

        val table = spark.table("non_bucketed_table")
        val nValues = table.select("j", "k").count()
        assert(nValues == nCount)
      }
    } catch {
      case e: Exception => fail("Failed due to exception: " + e)
    }
    // bucketed tables. This fails without the fix for SPARK-27100
    try {
      withTable("bucketed_table") {
        dfPartitioned.repartition(nShufflePartitions)
          .write
          .format("parquet")
          .partitionBy("i")
          .bucketBy(nBuckets, "j")
          .saveAsTable("bucketed_table")

        val table = spark.table("bucketed_table")
        val nValues = table.select("j", "k").count()
        assert(nValues == nCount)
      }
    } catch {
      case e: Exception => fail("Failed due to exception: " + e)
    }
  }

}
