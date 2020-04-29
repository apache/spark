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
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CoalesceBucketsSuite extends SQLTestUtils with SharedSparkSession {
  private def newRelation(numBuckets: Int): HadoopFsRelation = HadoopFsRelation(
    location = new InMemoryFileIndex(spark, Nil, Map.empty, None),
    partitionSchema = StructType(Seq(StructField("a", IntegerType))),
    dataSchema = StructType(Seq(StructField("a", IntegerType))),
    bucketSpec = Some(BucketSpec(numBuckets, Seq("a"), Seq("a"))),
    fileFormat = new ParquetFileFormat(),
    options = Map.empty)(spark)

  private def run(numBuckets1: Int, numBuckets2: Int, expectCoalescing: Boolean): Unit = {
    Seq((numBuckets1, numBuckets2), (numBuckets2, numBuckets1)).foreach { buckets =>
      val plan = CoalesceBucketsInJoin(
        Join(
          LogicalRelation(newRelation(buckets._1)),
          LogicalRelation(newRelation(buckets._2)),
          Inner,
          None,
          JoinHint.NONE))
      val coalesced = plan.collect { case c: CoalesceBuckets => c }
      if (expectCoalescing) {
        assert(coalesced.size == 1)
      } else {
        assert(coalesced.isEmpty)
      }
    }
  }

  test("bucket coalescing - basic") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> enabled.toString) {
        run(numBuckets1 = 4, numBuckets2 = 8, expectCoalescing = enabled)
      }
    }
  }

  test("bucket coalescing shouldn't be applied when the number of buckets are the same") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(numBuckets1 = 8, numBuckets2 = 8, expectCoalescing = false)
    }
  }

  test("number of bucket is not divisible by other number of bucket") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(numBuckets1 = 8, numBuckets2 = 3, expectCoalescing = false)
    }
  }

  test("the difference in the number of buckets is greater than max allowed") {
    withSQLConf(
      SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true",
      SQLConf.COALESCE_BUCKETS_IN_JOIN_MAX_NUM_BUCKETS_DIFF.key -> "2") {
      run(numBuckets1 = 8, numBuckets2 = 4, expectCoalescing = false)
    }
  }
}
