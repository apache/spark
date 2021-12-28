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

import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{FileSourceScanExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, DisableAdaptiveExecution}
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

class BucketedReadWithoutHiveSupportSuite
  extends BucketedReadSuite with SharedSparkSession {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    assert(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
  }
}


abstract class BucketedReadSuite extends QueryTest with SQLTestUtils with AdaptiveSparkPlanHelper {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING, true)
  }

  protected override def afterAll(): Unit = {
    spark.sessionState.conf.unsetConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING)
    super.afterAll()
  }

  private val maxI = 5
  private val maxJ = 13
  private lazy val df = (0 until 50).map(i => (i % maxI, i % maxJ, i.toString)).toDF("i", "j", "k")
  private lazy val nullDF = (for {
    i <- 0 to 50
    s <- Seq(null, "a", "b", "c", "d", "e", "f", null, "g")
  } yield (i % maxI, s, i % maxJ)).toDF("i", "j", "k")

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

      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {
        val bucketValue = Random.nextInt(maxI)
        val table = spark.table("bucketed_table").filter($"i" === bucketValue)
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
    val fileScan = collect(plan) { case f: FileSourceScanExec => f }
    assert(fileScan.nonEmpty, plan)
    fileScan.head
  }

  // To verify if the bucket pruning works, this function checks two conditions:
  //   1) Check if the pruned buckets (before filtering) are empty.
  //   2) Verify the final result is the same as the expected one
  private def checkPrunedAnswers(
      bucketSpec: BucketSpec,
      bucketValues: Seq[Any],
      filterCondition: Column,
      originalDataFrame: DataFrame): Unit = {
    // This test verifies parts of the plan. Disable whole stage codegen,
    // automatically bucketed scan, and filter push down for json data source.
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false",
      SQLConf.JSON_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      val bucketedDataFrame = spark.table("bucketed_table")
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

        withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {
          // Bucket pruning should still work without bucketed scan
          val planWithoutBucketedScan = bucketedDataFrame.filter(filterCondition)
            .queryExecution.executedPlan
          val fileScan = getFileScan(planWithoutBucketedScan)
          assert(!fileScan.bucketedScan, s"except no bucketed scan but found\n$fileScan")

          val bucketColumnType = bucketedDataFrame.schema.apply(bucketColumnIndex).dataType
          val rowsWithInvalidBuckets = fileScan.execute().filter(row => {
            // Return rows should have been pruned
            val bucketColumnValue = row.get(bucketColumnIndex, bucketColumnType)
            val bucketId = BucketingUtils.getBucketIdFromValue(
              bucketColumn, numBuckets, bucketColumnValue)
            !matchedBuckets.get(bucketId)
          }).collect()

          if (rowsWithInvalidBuckets.nonEmpty) {
            fail(s"Rows ${rowsWithInvalidBuckets.mkString(",")} should have been pruned from:\n" +
              s"$planWithoutBucketedScan")
          }
        }
      }

      val expectedDataFrame = originalDataFrame.filter(filterCondition).orderBy("i", "j", "k")
        .select("i", "j", "k")
      checkAnswer(
        bucketedDataFrame.filter(filterCondition).orderBy("i", "j", "k").select("i", "j", "k"),
        expectedDataFrame)

      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {
        checkAnswer(
          bucketedDataFrame.filter(filterCondition).orderBy("i", "j", "k").select("i", "j", "k"),
          expectedDataFrame)
      }
    }
  }

  test("read partitioning bucketed tables with bucket pruning filters") {
    withTable("bucketed_table") {
      val numBuckets = NumBucketsForPruningDF
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      df.write
        .format("json")
        .partitionBy("i")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      val bucketValue = Random.nextInt(maxJ)
      // Case 1: EqualTo
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = bucketValue :: Nil,
        filterCondition = $"j" === bucketValue,
        df)

      // Case 2: EqualNullSafe
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = bucketValue :: Nil,
        filterCondition = $"j" <=> bucketValue,
        df)

      // Case 3: In
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = Seq(bucketValue, bucketValue + 1, bucketValue + 2, bucketValue + 3),
        filterCondition = $"j".isin(bucketValue, bucketValue + 1, bucketValue + 2, bucketValue + 3),
        df)

      // Case 4: InSet
      val inSetExpr = expressions.InSet($"j".expr,
        Set(bucketValue, bucketValue + 1, bucketValue + 2, bucketValue + 3))
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = Seq(bucketValue, bucketValue + 1, bucketValue + 2, bucketValue + 3),
        filterCondition = Column(inSetExpr),
        df)
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

      val bucketValue = Random.nextInt(maxJ)
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = bucketValue :: Nil,
        filterCondition = $"j" === bucketValue,
        df)
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

  test("bucket pruning support IsNaN") {
    withTable("bucketed_table") {
      val numBuckets = NumBucketsForPruningNullDf
      val bucketSpec = BucketSpec(numBuckets, Seq("j"), Nil)
      val naNDF = nullDF.selectExpr("i", "cast(if(isnull(j), 'NaN', j) as double) as j", "k")
      // json does not support predicate push-down, and thus json is used here
      naNDF.write
        .format("json")
        .bucketBy(numBuckets, "j")
        .saveAsTable("bucketed_table")

      checkPrunedAnswers(
        bucketSpec,
        bucketValues = Double.NaN :: Nil,
        filterCondition = $"j".isNaN,
        naNDF)
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

      val bucketValue = Random.nextInt(maxJ)
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = bucketValue :: Nil,
        filterCondition = $"j" === bucketValue && $"k" > $"j",
        df)

      checkPrunedAnswers(
        bucketSpec,
        bucketValues = bucketValue :: Nil,
        filterCondition = $"j" === bucketValue && $"i" > bucketValue % 5,
        df)

      // check multiple bucket values OR condition
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = Seq(bucketValue, bucketValue + 1),
        filterCondition = $"j" === bucketValue || $"j" === (bucketValue + 1),
        df)

      // check bucket value and none bucket value OR condition
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = Nil,
        filterCondition = $"j" === bucketValue || $"i" === 0,
        df)

      // check AND condition in complex expression
      checkPrunedAnswers(
        bucketSpec,
        bucketValues = Seq(bucketValue),
        filterCondition = ($"i" === 0 || $"k" > $"j") && $"j" === bucketValue,
        df)
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
      expectedSort: Boolean = true,
      expectedNumOutputPartitions: Option[Int] = None)

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
    val BucketedTableTestSpec(
      bucketSpecLeft,
      numPartitionsLeft,
      shuffleLeft,
      sortLeft,
      numOutputPartitionsLeft) = bucketedTableTestSpecLeft
    val BucketedTableTestSpec(
      bucketSpecRight,
      numPartitionsRight,
      shuffleRight,
      sortRight,
      numOutputPartitionsRight) = bucketedTableTestSpecRight

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

        val joinOperator = if (joined.sqlContext.conf.adaptiveExecutionEnabled) {
          val executedPlan =
            joined.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
          assert(executedPlan.isInstanceOf[SortMergeJoinExec])
          executedPlan.asInstanceOf[SortMergeJoinExec]
        } else {
          val executedPlan = joined.queryExecution.executedPlan
          assert(executedPlan.isInstanceOf[SortMergeJoinExec])
          executedPlan.asInstanceOf[SortMergeJoinExec]
        }

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

        // check the output partitioning
        if (numOutputPartitionsLeft.isDefined) {
          assert(joinOperator.left.outputPartitioning.numPartitions ===
            numOutputPartitionsLeft.get)
        }
        if (numOutputPartitionsRight.isDefined) {
          assert(joinOperator.right.outputPartitioning.numPartitions ===
            numOutputPartitionsRight.get)
        }
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
      val aggregated = tbl.groupBy("i", "j").agg(max("k"))

      checkAnswer(
        aggregated.sort("i", "j"),
        df1.groupBy("i", "j").agg(max("k")).sort("i", "j"))

      assert(
        aggregated.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)
    }
  }

  test("sort should not be introduced when aliases are used") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      withTable("t") {
        df1.repartition(1).write.format("parquet").bucketBy(8, "i").sortBy("i").saveAsTable("t")
        val t1 = spark.table("t")
        val t2 = t1.selectExpr("i as ii")
        val plan = t1.join(t2, t1("i") === t2("ii")).queryExecution.executedPlan
        assert(plan.collect { case sort: SortExec => sort }.isEmpty)
      }
    }
  }

  test("bucket join should work with SubqueryAlias plan") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      withTable("t") {
        withView("v") {
          spark.range(20).selectExpr("id as i").write.bucketBy(8, "i").saveAsTable("t")
          sql("CREATE VIEW v AS SELECT * FROM t").collect()

          val plan = sql("SELECT * FROM t a JOIN v b ON a.i = b.i").queryExecution.executedPlan
          assert(plan.collect { case exchange: ShuffleExchangeExec => exchange }.isEmpty)
        }
      }
    }
  }

  test("avoid shuffle when grouping keys are a super-set of bucket keys") {
    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").saveAsTable("bucketed_table")
      val tbl = spark.table("bucketed_table")
      val aggregated = tbl.groupBy("i", "j").agg(max("k"))

      checkAnswer(
        aggregated.sort("i", "j"),
        df1.groupBy("i", "j").agg(max("k")).sort("i", "j"))

      assert(
        aggregated.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)
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
      BucketedTableTestSpec(Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))),
        numPartitions = 1, expectedShuffle = false)
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

      val aggregated = spark.table("bucketed_table").groupBy("i").count()
      val error = intercept[Exception] {
        aggregated.count()
      }

      assert(error.toString contains "Invalid bucket file")
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

  test("SPARK-29655 Read bucketed tables obeys spark.sql.shuffle.partitions") {
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "7")  {
      val bucketSpec = Some(BucketSpec(6, Seq("i", "j"), Nil))
      Seq(false, true).foreach { enableAdaptive =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> s"$enableAdaptive") {
          val bucketedTableTestSpecLeft = BucketedTableTestSpec(bucketSpec, expectedShuffle = false)
          val bucketedTableTestSpecRight = BucketedTableTestSpec(None, expectedShuffle = true)
          testBucketing(
            bucketedTableTestSpecLeft = bucketedTableTestSpecLeft,
            bucketedTableTestSpecRight = bucketedTableTestSpecRight,
            joinCondition = joinCondition(Seq("i", "j"))
          )
        }
      }
    }
  }

  test("SPARK-32767 Bucket join should work if SHUFFLE_PARTITIONS larger than bucket number") {
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "9",
      SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "10")  {

      val testSpec1 = BucketedTableTestSpec(
        Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))),
        numPartitions = 1,
        expectedShuffle = false,
        expectedSort = false,
        expectedNumOutputPartitions = Some(8))
      val testSpec2 = BucketedTableTestSpec(
        Some(BucketSpec(6, Seq("i", "j"), Seq("i", "j"))),
        numPartitions = 1,
        expectedShuffle = true,
        expectedSort = true,
        expectedNumOutputPartitions = Some(8))
      Seq(false, true).foreach { enableAdaptive =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> s"$enableAdaptive") {
          Seq((testSpec1, testSpec2), (testSpec2, testSpec1)).foreach { specs =>
            testBucketing(
              bucketedTableTestSpecLeft = specs._1,
              bucketedTableTestSpecRight = specs._2,
              joinCondition = joinCondition(Seq("i", "j")))
          }
        }
      }
    }
  }

  test("bucket coalescing eliminates shuffle") {
    withSQLConf(
      SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // The side with bucketedTableTestSpec1 will be coalesced to have 4 output partitions.
      // Currently, sort will be introduced for the side that is coalesced.
      val testSpec1 = BucketedTableTestSpec(
        Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))),
        numPartitions = 1,
        expectedShuffle = false,
        expectedSort = true,
        expectedNumOutputPartitions = Some(4))
      val testSpec2 = BucketedTableTestSpec(
        Some(BucketSpec(4, Seq("i", "j"), Seq("i", "j"))),
        numPartitions = 1,
        expectedShuffle = false,
        expectedSort = false,
        expectedNumOutputPartitions = Some(4))

      Seq((testSpec1, testSpec2), (testSpec2, testSpec1)).foreach { specs =>
        testBucketing(
          bucketedTableTestSpecLeft = specs._1,
          bucketedTableTestSpecRight = specs._2,
          joinCondition = joinCondition(Seq("i", "j")))
      }
    }
  }

  test("bucket coalescing is not satisfied") {
    def run(testSpec1: BucketedTableTestSpec, testSpec2: BucketedTableTestSpec): Unit = {
      Seq((testSpec1, testSpec2), (testSpec2, testSpec1)).foreach { specs =>
        testBucketing(
          bucketedTableTestSpecLeft = specs._1,
          bucketedTableTestSpecRight = specs._2,
          joinCondition = joinCondition(Seq("i", "j")))
      }
    }

    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "false") {
      // Coalescing buckets is disabled by a config.
      run(
        BucketedTableTestSpec(
          Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))), expectedShuffle = false),
        BucketedTableTestSpec(
          Some(BucketSpec(4, Seq("i", "j"), Seq("i", "j"))), expectedShuffle = true))
    }

    withSQLConf(
      SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true",
      SQLConf.COALESCE_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO.key -> "2") {
      // Coalescing buckets is not applied because the ratio of the number of buckets (3)
      // is greater than max allowed (2).
      run(
        BucketedTableTestSpec(
          Some(BucketSpec(12, Seq("i", "j"), Seq("i", "j"))), expectedShuffle = false),
        BucketedTableTestSpec(
          Some(BucketSpec(4, Seq("i", "j"), Seq("i", "j"))), expectedShuffle = true))
    }

    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(
        // Coalescing buckets is not applied because the bigger number of buckets (8) is not
        // divisible by the smaller number of buckets (7).
        BucketedTableTestSpec(
          Some(BucketSpec(8, Seq("i", "j"), Seq("i", "j"))), expectedShuffle = false),
        BucketedTableTestSpec(
          Some(BucketSpec(7, Seq("i", "j"), Seq("i", "j"))), expectedShuffle = true))
    }
  }

  test("bucket coalescing is applied when join expressions match with partitioning expressions",
    DisableAdaptiveExecution("Expected shuffle num mismatched")) {
    withTable("t1", "t2") {
      df1.write.format("parquet").bucketBy(8, "i", "j").saveAsTable("t1")
      df2.write.format("parquet").bucketBy(4, "i", "j").saveAsTable("t2")

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
        def verify(
            query: String,
            expectedNumShuffles: Int,
            expectedCoalescedNumBuckets: Option[Int]): Unit = {
          val plan = sql(query).queryExecution.executedPlan
          val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
          assert(shuffles.length == expectedNumShuffles)

          val scans = plan.collect {
            case f: FileSourceScanExec if f.optionalNumCoalescedBuckets.isDefined => f
          }
          if (expectedCoalescedNumBuckets.isDefined) {
            assert(scans.length == 1)
            assert(scans.head.optionalNumCoalescedBuckets == expectedCoalescedNumBuckets)
          } else {
            assert(scans.isEmpty)
          }
        }

        // Coalescing applied since join expressions match with the bucket columns.
        verify("SELECT * FROM t1 JOIN t2 ON t1.i = t2.i AND t1.j = t2.j", 0, Some(4))
        // Coalescing applied when columns are aliased.
        verify(
          "SELECT * FROM t1 JOIN (SELECT i AS x, j AS y FROM t2) ON t1.i = x AND t1.j = y",
          0,
          Some(4))
        // Coalescing is not applied when join expressions do not match with bucket columns.
        verify("SELECT * FROM t1 JOIN t2 ON t1.i = t2.i", 2, None)
      }
    }
  }
}
