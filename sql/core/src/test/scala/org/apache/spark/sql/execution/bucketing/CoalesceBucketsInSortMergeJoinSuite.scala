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
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{BinaryExecNode, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CoalesceBucketsInSortMergeJoinSuite extends SQLTestUtils with SharedSparkSession {
  case class BucketSetting(
      numBuckets: Int,
      expectedCoalescedNumBuckets: Option[Int])

  private def newRelation(numBuckets: Int): HadoopFsRelation = HadoopFsRelation(
    location = new InMemoryFileIndex(spark, Nil, Map.empty, None),
    partitionSchema = StructType(Seq(StructField("a", IntegerType))),
    dataSchema = StructType(Seq(StructField("a", IntegerType))),
    bucketSpec = Some(BucketSpec(numBuckets, Seq("a"), Seq("a"))),
    fileFormat = new ParquetFileFormat(),
    options = Map.empty)(spark)

  private def run(
      bucket1: BucketSetting,
      bucket2: BucketSetting,
      isSortMergeJoin: Boolean): Unit = {
    Seq((bucket1, bucket2), (bucket2, bucket1)).foreach { case (l, r) =>
      val lRelation = newRelation(l.numBuckets)
      val rRelation = newRelation(r.numBuckets)
      val lScan = FileSourceScanExec(
        lRelation, Nil, lRelation.dataSchema, Nil, None, None, Nil, None)
      val rScan = FileSourceScanExec(
        rRelation, Nil, rRelation.dataSchema, Nil, None, None, Nil, None)
      val join = if (isSortMergeJoin) {
        SortMergeJoinExec(Nil, Nil, Inner, None, lScan, rScan)
      } else {
        BroadcastHashJoinExec(Nil, Nil, Inner, BuildLeft, None, lScan, rScan)
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

      verify(l.expectedCoalescedNumBuckets, plan.asInstanceOf[BinaryExecNode].left)
      verify(r.expectedCoalescedNumBuckets, plan.asInstanceOf[BinaryExecNode].right)
    }
  }

  test("bucket coalescing - basic") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      run(BucketSetting(4, None), BucketSetting(8, Some(4)), isSortMergeJoin = true)
    }
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "false") {
      run(BucketSetting(4, None), BucketSetting(8, None), isSortMergeJoin = true)
    }
  }

  test("bucket coalescing should work only for sort merge join") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> enabled.toString) {
        run(BucketSetting(4, None), BucketSetting(8, None), isSortMergeJoin = false)
      }
    }
  }

  test("bucket coalescing shouldn't be applied when the number of buckets are the same") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      run(BucketSetting(8, None), BucketSetting(8, None), isSortMergeJoin = true)
    }
  }

  test("number of bucket is not divisible by other number of bucket") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      run(BucketSetting(3, None), BucketSetting(8, None), isSortMergeJoin = true)
    }
  }

  test("the ratio of the number of buckets is greater than max allowed") {
    withSQLConf(
      SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_ENABLED.key -> "true",
      SQLConf.COALESCE_BUCKETS_IN_SORT_MERGE_JOIN_MAX_BUCKET_RATIO.key -> "2") {
      run(BucketSetting(4, None), BucketSetting(16, None), isSortMergeJoin = true)
    }
  }
}
