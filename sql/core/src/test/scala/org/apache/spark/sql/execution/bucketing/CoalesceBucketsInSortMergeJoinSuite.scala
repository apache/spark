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

package org.apache.spark.sql.execution.bucketing

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{BinaryExecNode, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructType}

class CoalesceBucketsInSortMergeJoinSuite extends SQLTestUtils with SharedSparkSession {
  case class RelationSetting(
      cols: Seq[Attribute],
      numBuckets: Int,
      expectedCoalescedNumBuckets: Option[Int])

  object RelationSetting {
    def apply(numBuckets: Int, expectedCoalescedNumBuckets: Option[Int]): RelationSetting = {
      val cols = Seq(AttributeReference("i", IntegerType)())
      RelationSetting(cols, numBuckets, expectedCoalescedNumBuckets)
    }
  }

  case class JoinSetting(
      leftKeys: Seq[Attribute],
      rightKeys: Seq[Attribute],
      leftRelation: RelationSetting,
      rightRelation: RelationSetting,
      isSortMergeJoin: Boolean)

  object JoinSetting {
    def apply(l: RelationSetting, r: RelationSetting, isSortMergeJoin: Boolean): JoinSetting = {
      JoinSetting(l.cols, r.cols, l, r, isSortMergeJoin)
    }
  }

  private def newFileSourceScanExec(setting: RelationSetting): FileSourceScanExec = {
    val relation = HadoopFsRelation(
      location = new InMemoryFileIndex(spark, Nil, Map.empty, None),
      partitionSchema = PartitionSpec.emptySpec.partitionColumns,
      dataSchema = StructType.fromAttributes(setting.cols),
      bucketSpec = Some(BucketSpec(setting.numBuckets, setting.cols.map(_.name), Nil)),
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
    FileSourceScanExec(relation, setting.cols, relation.dataSchema, Nil, None, None, Nil, None)
  }

  private def run(setting: JoinSetting): Unit = {
    val swappedSetting = setting.copy(
      leftKeys = setting.rightKeys,
      rightKeys = setting.leftKeys,
      leftRelation = setting.rightRelation,
      rightRelation = setting.leftRelation)

    Seq(setting, swappedSetting).foreach { case s =>
      val lScan = newFileSourceScanExec(s.leftRelation)
      val rScan = newFileSourceScanExec(s.rightRelation)
      val join = if (s.isSortMergeJoin) {
        SortMergeJoinExec(s.leftKeys, s.rightKeys, Inner, None, lScan, rScan)
      } else {
        BroadcastHashJoinExec(
          s.leftKeys, s.rightKeys, Inner, BuildLeft, None, lScan, rScan)
      }

      val plan = CoalesceBucketsInSortMergeJoin(spark.sessionState.conf)(join)

      def verify(expected: Option[Int], subPlan: SparkPlan): Unit = {
        val coalesced = subPlan.collect {
          case f: FileSourceScanExec if f.optionalNumCoalescedBuckets.nonEmpty =>
            f.optionalNumCoalescedBuckets.get
        }
        if (expected.isDefined) {
          assert(coalesced.size == 1 && coalesced(0) == expected.get)
        } else {
          assert(coalesced.isEmpty)
        }
      }

      verify(s.leftRelation.expectedCoalescedNumBuckets, plan.asInstanceOf[BinaryExecNode].left)
      verify(s.rightRelation.expectedCoalescedNumBuckets, plan.asInstanceOf[BinaryExecNode].right)
    }
  }

  test("bucket coalescing - basic") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, Some(4)), isSortMergeJoin = true))
    }
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "false") {
      run(JoinSetting(RelationSetting(4, None), RelationSetting(8, None), isSortMergeJoin = true))
    }
  }

  test("bucket coalescing should work only for sort merge join") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> enabled.toString) {
        run(JoinSetting(
          RelationSetting(4, None), RelationSetting(8, None), isSortMergeJoin = false))
      }
    }
  }

  test("bucket coalescing shouldn't be applied when the number of buckets are the same") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(RelationSetting(8, None), RelationSetting(8, None), isSortMergeJoin = true))
    }
  }

  test("number of bucket is not divisible by other number of bucket") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(RelationSetting(3, None), RelationSetting(8, None), isSortMergeJoin = true))
    }
  }

  test("the ratio of the number of buckets is greater than max allowed") {
    withSQLConf(
      SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true",
      SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_MAX_BUCKET_RATIO.key -> "2") {
      run(JoinSetting(RelationSetting(4, None), RelationSetting(16, None), isSortMergeJoin = true))
    }
  }

  test("join keys should match with output partitioning") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      val lCols = Seq(
        AttributeReference("l1", IntegerType)(),
        AttributeReference("l2", IntegerType)())
      val rCols = Seq(
        AttributeReference("r1", IntegerType)(),
        AttributeReference("r2", IntegerType)())

      val lRel = RelationSetting(lCols, 4, None)
      val rRel = RelationSetting(rCols, 8, None)

      // The following should not be coalesced because join keys do not match with output
      // partitioning (missing one expression).
      run(JoinSetting(
        leftKeys = Seq(lCols.head),
        rightKeys = Seq(rCols.head),
        leftRelation = lRel,
        rightRelation = rRel,
        isSortMergeJoin = true))

      // The following should not be coalesced because join keys do not match with output
      // partitioning (more expressions).
      run(JoinSetting(
        leftKeys = lCols :+ AttributeReference("l3", IntegerType)(),
        rightKeys = rCols :+ AttributeReference("r3", IntegerType)(),
        leftRelation = lRel,
        rightRelation = rRel,
        isSortMergeJoin = true))

      // The following will be coalesced since ordering should not matter because it will be
      // adjusted in `EnsureRequirements`.
      run(JoinSetting(
        leftKeys = lCols.reverse,
        rightKeys = rCols.reverse,
        leftRelation = lRel,
        rightRelation = RelationSetting(rCols, 8, Some(4)),
        isSortMergeJoin = true))
    }
  }

  test("FileSourceScanExec's metadata should be updated with coalesced info") {
    val scan = newFileSourceScanExec(RelationSetting(8, None))
    val value = scan.metadata("SelectedBucketsCount")
    assert(value === "8 out of 8")

    val scanWithCoalescing = scan.copy(optionalNumCoalescedBuckets = Some(4))
    val valueWithCoalescing = scanWithCoalescing.metadata("SelectedBucketsCount")
    assert(valueWithCoalescing == "8 out of 8 (Coalesced to 4)")
  }
}
