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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{BinaryExecNode, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class CoalesceBucketsInJoinSuite extends SharedSparkSession with AdaptiveSparkPlanHelper {
  private val SORT_MERGE_JOIN = "sortMergeJoin"
  private val SHUFFLED_HASH_JOIN = "shuffledHashJoin"
  private val BROADCAST_HASH_JOIN = "broadcastHashJoin"

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
      joinOperator: String,
      shjBuildSide: Option[BuildSide])

  object JoinSetting {
    def apply(
        l: RelationSetting,
        r: RelationSetting,
        joinOperator: String = SORT_MERGE_JOIN,
        shjBuildSide: Option[BuildSide] = None): JoinSetting = {
      JoinSetting(l.cols, r.cols, l, r, joinOperator, shjBuildSide)
    }
  }

  private def newFileSourceScanExec(setting: RelationSetting): FileSourceScanExec = {
    val relation = HadoopFsRelation(
      location = new InMemoryFileIndex(spark, Nil, Map.empty, None),
      partitionSchema = PartitionSpec.emptySpec.partitionColumns,
      dataSchema = DataTypeUtils.fromAttributes(setting.cols),
      bucketSpec = Some(BucketSpec(setting.numBuckets, setting.cols.map(_.name), Nil)),
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
    FileSourceScanExec(relation, None, setting.cols, relation.dataSchema, Nil, None, None, Nil,
      None)
  }

  private def run(setting: JoinSetting): Unit = {
    val swappedSetting = setting.copy(
      leftKeys = setting.rightKeys,
      rightKeys = setting.leftKeys,
      leftRelation = setting.rightRelation,
      rightRelation = setting.leftRelation)

    val settings = if (setting.joinOperator != SHUFFLED_HASH_JOIN) {
      Seq(setting, swappedSetting)
    } else {
      Seq(setting)
    }
    settings.foreach { s =>
      val lScan = newFileSourceScanExec(s.leftRelation)
      val rScan = newFileSourceScanExec(s.rightRelation)
      val join = if (s.joinOperator == SORT_MERGE_JOIN) {
        SortMergeJoinExec(s.leftKeys, s.rightKeys, Inner, None, lScan, rScan)
      } else if (s.joinOperator == SHUFFLED_HASH_JOIN) {
        ShuffledHashJoinExec(s.leftKeys, s.rightKeys, Inner, s.shjBuildSide.get, None, lScan, rScan)
      } else {
        BroadcastHashJoinExec(
          s.leftKeys, s.rightKeys, Inner, BuildLeft, None, lScan, rScan)
      }

      val plan = CoalesceBucketsInJoin(join)

      def verify(expected: Option[Int], subPlan: SparkPlan): Unit = {
        val coalesced = subPlan.collect {
          case f: FileSourceScanExec if f.optionalNumCoalescedBuckets.nonEmpty =>
            f.optionalNumCoalescedBuckets.get
        }
        if (expected.isDefined) {
          assert(coalesced.size == 1 && coalesced.head == expected.get)
        } else {
          assert(coalesced.isEmpty)
        }
      }

      verify(s.leftRelation.expectedCoalescedNumBuckets, plan.asInstanceOf[BinaryExecNode].left)
      verify(s.rightRelation.expectedCoalescedNumBuckets, plan.asInstanceOf[BinaryExecNode].right)
    }
  }

  test("bucket coalescing - basic") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, Some(4)), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, Some(4)), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }

    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "false") {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, None), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }
  }

  test("bucket coalescing should work only for sort merge join and shuffled hash join") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> enabled.toString) {
        run(JoinSetting(
          RelationSetting(4, None), RelationSetting(8, None), joinOperator = BROADCAST_HASH_JOIN))
      }
    }
  }

  test("bucket coalescing shouldn't be applied to shuffled hash join build side") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildRight)))
    }
  }

  test("bucket coalescing shouldn't be applied when the number of buckets are the same") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(
        RelationSetting(8, None), RelationSetting(8, None), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(8, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }
  }

  test("number of bucket is not divisible by other number of bucket") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      run(JoinSetting(
        RelationSetting(3, None), RelationSetting(8, None), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(3, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }
  }

  test("the ratio of the number of buckets is greater than max allowed") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true",
      SQLConf.COALESCE_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO.key -> "2") {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(16, None), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(16, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }
  }

  test("join keys should match with output partitioning") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
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
        joinOperator = SORT_MERGE_JOIN,
        shjBuildSide = None))

      run(JoinSetting(
        leftKeys = Seq(lCols.head),
        rightKeys = Seq(rCols.head),
        leftRelation = lRel,
        rightRelation = rRel,
        joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))

      // The following should not be coalesced because join keys do not match with output
      // partitioning (more expressions).
      run(JoinSetting(
        leftKeys = lCols :+ AttributeReference("l3", IntegerType)(),
        rightKeys = rCols :+ AttributeReference("r3", IntegerType)(),
        leftRelation = lRel,
        rightRelation = rRel,
        joinOperator = SORT_MERGE_JOIN,
        shjBuildSide = None))

      run(JoinSetting(
        leftKeys = lCols :+ AttributeReference("l3", IntegerType)(),
        rightKeys = rCols :+ AttributeReference("r3", IntegerType)(),
        leftRelation = lRel,
        rightRelation = rRel,
        joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))

      // The following will be coalesced since ordering should not matter because it will be
      // adjusted in `EnsureRequirements`.
      run(JoinSetting(
        leftKeys = lCols.reverse,
        rightKeys = rCols.reverse,
        leftRelation = lRel,
        rightRelation = RelationSetting(rCols, 8, Some(4)),
        joinOperator = SORT_MERGE_JOIN,
        shjBuildSide = None))

      run(JoinSetting(
        leftKeys = lCols.reverse,
        rightKeys = rCols.reverse,
        leftRelation = lRel,
        rightRelation = RelationSetting(rCols, 8, Some(4)),
        joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))

      run(JoinSetting(
        leftKeys = rCols.reverse,
        rightKeys = lCols.reverse,
        leftRelation = RelationSetting(rCols, 8, Some(4)),
        rightRelation = lRel,
        joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildRight)))
    }
  }

  test("SPARK-60382: broadcast join coalescing uses the streamed side bucket count") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      def scan(cols: Seq[Attribute], numBuckets: Int): FileSourceScanExec = {
        val relation = HadoopFsRelation(
          location = new InMemoryFileIndex(spark, Nil, Map.empty, None),
          partitionSchema = PartitionSpec.emptySpec.partitionColumns,
          dataSchema = DataTypeUtils.fromAttributes(cols),
          bucketSpec = Some(BucketSpec(numBuckets, Seq("i"), Nil)),
          fileFormat = new ParquetFileFormat(),
          options = Map.empty)(spark)
        FileSourceScanExec(
          relation, None, cols, relation.dataSchema, Nil, None, None, Nil, None)
      }

      def cols(): Seq[Attribute] = Seq(
        AttributeReference("i", IntegerType)(),
        AttributeReference("k", IntegerType)())

      def coalesced(plan: SparkPlan): Seq[(Int, Option[Int])] = {
        plan.collect { case f: FileSourceScanExec =>
          (f.relation.bucketSpec.get.numBuckets, f.optionalNumCoalescedBuckets)
        }
      }

      // Streamed side is 100 buckets. 100 % 8 != 0, so nothing is coalesced,
      // including the 16-bucket build scan that sits on the left.
      val build = cols()
      val stream = cols()
      val other = Seq(AttributeReference("i", IntegerType)())
      val hashJoin = BroadcastHashJoinExec(
        Seq(build(1)), Seq(stream(1)), Inner, BuildLeft, None,
        scan(build, 16), scan(stream, 100))
      val plan = CoalesceBucketsInJoin(SortMergeJoinExec(
        Seq(stream.head), Seq(other.head), Inner, None, hashJoin, scan(other, 8)))
      assert(coalesced(plan) == Seq((16, None), (100, None), (8, None)))

      val nestedLoop = BroadcastNestedLoopJoinExec(
        scan(build, 16), scan(stream, 100), BuildLeft, Inner, None)
      val nestedPlan = CoalesceBucketsInJoin(SortMergeJoinExec(
        Seq(stream.head), Seq(other.head), Inner, None, nestedLoop, scan(other, 8)))
      assert(coalesced(nestedPlan) == Seq((16, None), (100, None), (8, None)))

      // Streamed side is 16 buckets and the other input is 8. Only the
      // streamed scan is rewritten to 8. The 100-bucket build scan is not.
      val buildDiv = cols()
      val streamDiv = cols()
      val otherDiv = Seq(AttributeReference("i", IntegerType)())
      val hashJoinDiv = BroadcastHashJoinExec(
        Seq(buildDiv(1)), Seq(streamDiv(1)), Inner, BuildLeft, None,
        scan(buildDiv, 100), scan(streamDiv, 16))
      val planDiv = CoalesceBucketsInJoin(SortMergeJoinExec(
        Seq(streamDiv.head), Seq(otherDiv.head), Inner, None,
        hashJoinDiv, scan(otherDiv, 8)))
      assert(coalesced(planDiv) == Seq((100, None), (16, Some(8)), (8, None)))

      val nestedLoopDiv = BroadcastNestedLoopJoinExec(
        scan(buildDiv, 100), scan(streamDiv, 16), BuildLeft, Inner, None)
      val nestedPlanDiv = CoalesceBucketsInJoin(SortMergeJoinExec(
        Seq(streamDiv.head), Seq(otherDiv.head), Inner, None,
        nestedLoopDiv, scan(otherDiv, 8)))
      assert(coalesced(nestedPlanDiv) == Seq((100, None), (16, Some(8)), (8, None)))
    }
  }

  test("SPARK-60382: a bucketed join inside a broadcast build side keeps its own bucket count") {
    withSQLConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
      def scan(cols: Seq[Attribute], numBuckets: Int): FileSourceScanExec = {
        val relation = HadoopFsRelation(
          location = new InMemoryFileIndex(spark, Nil, Map.empty, None),
          partitionSchema = PartitionSpec.emptySpec.partitionColumns,
          dataSchema = DataTypeUtils.fromAttributes(cols),
          bucketSpec = Some(BucketSpec(numBuckets, Seq("i"), Nil)),
          fileFormat = new ParquetFileFormat(),
          options = Map.empty)(spark)
        FileSourceScanExec(
          relation, None, cols, relation.dataSchema, Nil, None, None, Nil, None)
      }

      def coalesced(plan: SparkPlan): Seq[(Int, Option[Int])] = {
        plan.collect { case f: FileSourceScanExec =>
          (f.relation.bucketSpec.get.numBuckets, f.optionalNumCoalescedBuckets)
        }
      }

      // SMJ(s.i = o.i)(BHJ(BuildRight, s16, SMJ(a.i = c.i)(a100, c8)), o8). The outer join
      // coalesces s16 (the streamed side of the inner broadcast join) to 8. The inner
      // sort-merge join is then judged on its own: 100 is not divisible by 8, so a100 and
      // c8 are left untouched even though they sit inside the outer join's coalesced side.
      val s = Seq(
        AttributeReference("i", IntegerType)(), AttributeReference("k", IntegerType)())
      val a = Seq(
        AttributeReference("i", IntegerType)(), AttributeReference("k", IntegerType)())
      val c = Seq(AttributeReference("i", IntegerType)())
      val o = Seq(AttributeReference("i", IntegerType)())

      val inner = SortMergeJoinExec(
        Seq(a.head), Seq(c.head), Inner, None, scan(a, 100), scan(c, 8))
      val bhj = BroadcastHashJoinExec(
        Seq(s(1)), Seq(a(1)), Inner, BuildRight, None, scan(s, 16), inner)
      val plan = CoalesceBucketsInJoin(SortMergeJoinExec(
        Seq(s.head), Seq(o.head), Inner, None, bhj, scan(o, 8)))
      assert(coalesced(plan) == Seq((16, Some(8)), (100, None), (8, None), (8, None)))
    }
  }

  test("SPARK-60382: broadcast hash join bucket coalescing keeps the sort-merge match") {
    Seq(true, false).foreach { aqeEnabled =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString,
        SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
        withTable("dim16", "fact100", "other8") {
          withTempView("picked") {
            spark.sql(
              """CREATE TABLE dim16 (i INT, k INT) USING parquet
                |CLUSTERED BY (i) INTO 16 BUCKETS""".stripMargin)
            spark.sql(
              """CREATE TABLE fact100 (i INT, k INT, v STRING) USING parquet
                |CLUSTERED BY (i) INTO 100 BUCKETS""".stripMargin)
            spark.sql(
              """CREATE TABLE other8 (i INT, v STRING) USING parquet
                |CLUSTERED BY (i) INTO 8 BUCKETS""".stripMargin)
            spark.sql("INSERT INTO dim16 VALUES (0, 1)")
            spark.sql(
              """CREATE OR REPLACE TEMP VIEW picked AS
                |SELECT CAST(id AS INT) AS i
                |FROM range(0, 4000)
                |WHERE pmod(pmod(hash(CAST(id AS INT)), 100), 8)
                |      <> pmod(hash(CAST(id AS INT)), 8)
                |ORDER BY id
                |LIMIT 1""".stripMargin)
            spark.sql("INSERT INTO fact100 SELECT i, 1, 'fact' FROM picked")
            spark.sql("INSERT INTO other8 SELECT i, 'other' FROM picked")
            val key = spark.sql("SELECT i FROM picked").head().getInt(0)

            val query =
              """SELECT /*+ BROADCAST(dim16), MERGE(fact100, other8) */
                |  fact100.i, fact100.v, other8.v
                |FROM dim16
                |JOIN fact100 ON dim16.k = fact100.k
                |JOIN other8 ON fact100.i = other8.i""".stripMargin
            val df = spark.sql(query)
            val executed = df.queryExecution.executedPlan
            val scans = collect(executed) { case f: FileSourceScanExec =>
              (f.relation.bucketSpec.map(_.numBuckets), f.optionalNumCoalescedBuckets)
            }
            assert(scans == Seq((Some(16), None), (Some(100), None), (Some(8), None)), executed)
            assert(collectFirst(executed) { case b: BroadcastHashJoinExec => b.buildSide }
              .contains(BuildLeft), executed)
            checkAnswer(df, Seq(Row(key, "fact", "other")))
          }
        }
      }
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
