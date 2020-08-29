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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{BinaryExecNode, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructType}

class CoalesceOrRepartitionBucketsInJoinSuite extends SQLTestUtils with SharedSparkSession {
  private val SORT_MERGE_JOIN = "sortMergeJoin"
  private val SHUFFLED_HASH_JOIN = "shuffledHashJoin"
  private val BROADCAST_HASH_JOIN = "broadcastHashJoin"

  case class RelationSetting(
      cols: Seq[Attribute],
      numBuckets: Int,
      expectedNumBuckets: Option[Int])

  object RelationSetting {
    def apply(numBuckets: Int, expectedNumBuckets: Option[Int]): RelationSetting = {
      val cols = Seq(AttributeReference("i", IntegerType)())
      RelationSetting(cols, numBuckets, expectedNumBuckets)
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
      rightRelation = setting.leftRelation,
      shjBuildSide = setting.shjBuildSide.map {
        case BuildLeft => BuildRight
        case BuildRight => BuildLeft
      })

    val settings = Seq(setting, swappedSetting)

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

      val plan = CoalesceOrRepartitionBucketsInJoin(spark.sessionState.conf)(join)

      def verify(expected: Option[Int], subPlan: SparkPlan): Unit = {
        val optionalNewNumBuckets = subPlan.collect {
          case f: FileSourceScanExec if f.optionalNewNumBuckets.nonEmpty =>
            f.optionalNewNumBuckets.get
        }
        if (expected.isDefined) {
          assert(optionalNewNumBuckets.size == 1 && optionalNewNumBuckets.head == expected.get)
        } else {
          assert(optionalNewNumBuckets.isEmpty)
        }
      }

      verify(s.leftRelation.expectedNumBuckets, plan.asInstanceOf[BinaryExecNode].left)
      verify(s.rightRelation.expectedNumBuckets, plan.asInstanceOf[BinaryExecNode].right)
    }
  }

  test("bucket coalescing - basic") {
    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.COALESCE.toString) {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, Some(4)), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, Some(4)), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }

    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.OFF.toString) {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, None), joinOperator = SORT_MERGE_JOIN))
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildLeft)))
    }
  }

  test("bucket repartitioning - basic") {
    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString) {
      run(JoinSetting(
        RelationSetting(8, None), RelationSetting(4, Some(8)), joinOperator = SORT_MERGE_JOIN))
      Seq(BuildLeft, BuildRight).foreach { buildSide =>
        run(JoinSetting(
          RelationSetting(8, None), RelationSetting(4, Some(8)), joinOperator = SHUFFLED_HASH_JOIN,
          shjBuildSide = Some(buildSide)))
      }
    }

    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.OFF.toString) {
      run(JoinSetting(
        RelationSetting(8, None), RelationSetting(4, None), joinOperator = SORT_MERGE_JOIN))
      Seq(BuildLeft, BuildRight).foreach { buildSide =>
        run(JoinSetting(
          RelationSetting(8, None), RelationSetting(4, None), joinOperator = SHUFFLED_HASH_JOIN,
          shjBuildSide = Some(buildSide)))
      }
    }
  }

  test("bucket coalesce/repartition should work only for sort merge join and shuffled hash join") {
    Seq(SQLConf.BucketReadStrategyInJoin.COALESCE.toString,
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString).foreach { strategy =>
      withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key -> strategy) {
        run(JoinSetting(
          RelationSetting(4, None), RelationSetting(8, None), joinOperator = BROADCAST_HASH_JOIN))
      }
    }
  }

  test("bucket coalescing shouldn't be applied to shuffled hash join build side") {
    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.COALESCE.toString) {
      run(JoinSetting(
        RelationSetting(4, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
        shjBuildSide = Some(BuildRight)))
    }
  }

  test("bucket coalesce/repartition shouldn't be applied when the number of buckets are the same") {
    Seq(SQLConf.BucketReadStrategyInJoin.COALESCE.toString,
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString).foreach { strategy =>
      withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key -> strategy) {
        run(JoinSetting(
          RelationSetting(8, None), RelationSetting(8, None), joinOperator = SORT_MERGE_JOIN))
        run(JoinSetting(
          RelationSetting(8, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
          shjBuildSide = Some(BuildLeft)))
      }
    }
  }

  test("number of bucket is not divisible by other number of bucket") {
    Seq(SQLConf.BucketReadStrategyInJoin.COALESCE.toString,
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString).foreach { strategy =>
      withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key -> strategy) {
        run(JoinSetting(
          RelationSetting(3, None), RelationSetting(8, None), joinOperator = SORT_MERGE_JOIN))
        run(JoinSetting(
          RelationSetting(3, None), RelationSetting(8, None), joinOperator = SHUFFLED_HASH_JOIN,
          shjBuildSide = Some(BuildLeft)))
      }
    }
  }

  test("the ratio of the number of buckets is greater than max allowed") {
    Seq(SQLConf.BucketReadStrategyInJoin.COALESCE.toString,
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString).foreach { strategy =>
      withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key -> strategy,
        SQLConf.BUCKET_READ_STRATEGY_IN_JOIN_MAX_BUCKET_RATIO.key -> "2") {
        run(JoinSetting(
          RelationSetting(4, None), RelationSetting(16, None), joinOperator = SORT_MERGE_JOIN))
        run(JoinSetting(
          RelationSetting(4, None), RelationSetting(16, None), joinOperator = SHUFFLED_HASH_JOIN,
          shjBuildSide = Some(BuildLeft)))
      }
    }
  }

  test("join keys should match with output partitioning") {
    val lCols = Seq(
      AttributeReference("l1", IntegerType)(),
      AttributeReference("l2", IntegerType)())
    val rCols = Seq(
      AttributeReference("r1", IntegerType)(),
      AttributeReference("r2", IntegerType)())

    val lRel = RelationSetting(lCols, 4, None)
    val rRel = RelationSetting(rCols, 8, None)

    Seq(SQLConf.BucketReadStrategyInJoin.COALESCE.toString,
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString).foreach { strategy =>
      withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key -> strategy) {
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
      }
    }

    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.COALESCE.toString) {
      // The following will be coalesced since ordering should not matter because it will be
      // adjusted in `EnsureRequirements`.
      val setting = JoinSetting(
        leftKeys = lCols.reverse,
        rightKeys = rCols.reverse,
        leftRelation = lRel,
        rightRelation = RelationSetting(rCols, 8, Some(4)),
        joinOperator = "",
        shjBuildSide = None)

      run(setting.copy(joinOperator = SORT_MERGE_JOIN))
      run(setting.copy(joinOperator = SHUFFLED_HASH_JOIN, shjBuildSide = Some(BuildLeft)))
    }

    withSQLConf(SQLConf.BUCKET_READ_STRATEGY_IN_JOIN.key ->
      SQLConf.BucketReadStrategyInJoin.REPARTITION.toString) {
      // The following will be repartitioned since ordering should not matter because it will be
      // adjusted in `EnsureRequirements`.
      val setting = JoinSetting(
        leftKeys = lCols.reverse,
        rightKeys = rCols.reverse,
        leftRelation = RelationSetting(lCols, 4, Some(8)),
        rightRelation = rRel,
        joinOperator = "",
        shjBuildSide = None)

      run(setting.copy(joinOperator = SORT_MERGE_JOIN))
      Seq(BuildLeft, BuildRight).foreach { buildSide =>
        run(setting.copy(joinOperator = SHUFFLED_HASH_JOIN, shjBuildSide = Some(buildSide)))
      }
    }
  }

  test("FileSourceScanExec's metadata should be updated with coalesced/repartitioned info") {
    val scan = newFileSourceScanExec(RelationSetting(8, None))
    val value = scan.metadata("SelectedBucketsCount")
    assert(value === "8 out of 8")

    val scanWithCoalescing = scan.copy(optionalNewNumBuckets = Some(4))
    val metadataWithCoalescing = scanWithCoalescing.metadata("SelectedBucketsCount")
    assert(metadataWithCoalescing == "8 out of 8 (Coalesced to 4)")

    val scanWithRepartitioning = scan.copy(optionalNewNumBuckets = Some(16))
    val metadataWithRepartitioning = scanWithRepartitioning.metadata("SelectedBucketsCount")
    assert(metadataWithRepartitioning == "8 out of 8 (Repartitioned to 16)")
  }
}
