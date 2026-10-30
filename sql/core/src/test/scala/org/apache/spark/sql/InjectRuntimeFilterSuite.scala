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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, DynamicPruningExpression, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, ProjectedBroadcastValueSubqueryExec, ReusedSubqueryExec, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEPropagateEmptyRelation}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.planmerging.MergeSubplans
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel

class InjectRuntimeFilterSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    val schema = new StructType().add("a1", IntegerType, nullable = true)
      .add("b1", IntegerType, nullable = true)
      .add("c1", IntegerType, nullable = true)
      .add("d1", IntegerType, nullable = true)
      .add("e1", IntegerType, nullable = true)
      .add("f1", IntegerType, nullable = true)

    val data1 = Seq(Seq(null, 47, null, 4, 6, 48),
      Seq(73, 63, null, 92, null, null),
      Seq(76, 10, 74, 98, 37, 5),
      Seq(0, 63, null, null, null, null),
      Seq(15, 77, null, null, null, null),
      Seq(null, 57, 33, 55, null, 58),
      Seq(4, 0, 86, null, 96, 14),
      Seq(28, 16, 58, null, null, null),
      Seq(1, 88, null, 8, null, 79),
      Seq(59, null, null, null, 20, 25),
      Seq(1, 50, null, 94, 94, null),
      Seq(null, null, null, 67, 51, 57),
      Seq(77, 50, 8, 90, 16, 21),
      Seq(34, 28, null, 5, null, 64),
      Seq(null, null, 88, 11, 63, 79),
      Seq(92, 94, 23, 1, null, 64),
      Seq(57, 56, null, 83, null, null),
      Seq(null, 35, 8, 35, null, 70),
      Seq(null, 8, null, 35, null, 87),
      Seq(9, null, null, 60, null, 5),
      Seq(null, 15, 66, null, 83, null))
    val rdd1 = spark.sparkContext.parallelize(data1)
    val rddRow1 = rdd1.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow1, schema).write.saveAsTable("bf1")

    val schema2 = new StructType().add("a2", IntegerType, nullable = true)
      .add("b2", IntegerType, nullable = true)
      .add("c2", IntegerType, nullable = true)
      .add("d2", IntegerType, nullable = true)
      .add("e2", IntegerType, nullable = true)
      .add("f2", IntegerType, nullable = true)


    val data2 = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd2 = spark.sparkContext.parallelize(data2)
    val rddRow2 = rdd2.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow2, schema2).write.saveAsTable("bf2")

    val schema3 = new StructType().add("a3", IntegerType, nullable = true)
      .add("b3", IntegerType, nullable = true)
      .add("c3", IntegerType, nullable = true)
      .add("d3", IntegerType, nullable = true)
      .add("e3", IntegerType, nullable = true)
      .add("f3", IntegerType, nullable = true)

    val data3 = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd3 = spark.sparkContext.parallelize(data3)
    val rddRow3 = rdd3.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow3, schema3).write.saveAsTable("bf3")


    val schema4 = new StructType().add("a4", IntegerType, nullable = true)
      .add("b4", IntegerType, nullable = true)
      .add("c4", IntegerType, nullable = true)
      .add("d4", IntegerType, nullable = true)
      .add("e4", IntegerType, nullable = true)
      .add("f4", IntegerType, nullable = true)

    val data4 = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd4 = spark.sparkContext.parallelize(data4)
    val rddRow4 = rdd4.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow4, schema4).write.saveAsTable("bf4")

    val schema5part = new StructType().add("a5", IntegerType, nullable = true)
      .add("b5", IntegerType, nullable = true)
      .add("c5", IntegerType, nullable = true)
      .add("d5", IntegerType, nullable = true)
      .add("e5", IntegerType, nullable = true)
      .add("f5", IntegerType, nullable = true)

    val data5part = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd5part = spark.sparkContext.parallelize(data5part)
    val rddRow5part = rdd5part.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow5part, schema5part).write.partitionBy("f5")
      .saveAsTable("bf5part")
    spark.createDataFrame(rddRow5part, schema5part).filter("a5 > 30")
      .write.partitionBy("f5")
      .saveAsTable("bf5filtered")

    sql("analyze table bf1 compute statistics for columns a1, b1, c1, d1, e1, f1")
    sql("analyze table bf2 compute statistics for columns a2, b2, c2, d2, e2, f2")
    sql("analyze table bf3 compute statistics for columns a3, b3, c3, d3, e3, f3")
    sql("analyze table bf4 compute statistics for columns a4, b4, c4, d4, e4, f4")
    sql("analyze table bf5part compute statistics for columns a5, b5, c5, d5, e5, f5")
    sql("analyze table bf5filtered compute statistics for columns a5, b5, c5, d5, e5, f5")

    // `MergeScalarSubqueries` can duplicate subqueries in the optimized plan and would make testing
    // complicated.
    conf.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, MergeSubplans.ruleName)
  }

  protected override def afterAll(): Unit = try {
    conf.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
      SQLConf.OPTIMIZER_EXCLUDED_RULES.defaultValueString)

    sql("DROP TABLE IF EXISTS bf1")
    sql("DROP TABLE IF EXISTS bf2")
    sql("DROP TABLE IF EXISTS bf3")
    sql("DROP TABLE IF EXISTS bf4")
    sql("DROP TABLE IF EXISTS bf5part")
    sql("DROP TABLE IF EXISTS bf5filtered")
  } finally {
    super.afterAll()
  }

  def checkWithAndWithoutFeatureEnabled(
      query: String,
      shouldReplace: Boolean,
      runtimeFilterNum: Int = 1): Unit = {
    var planDisabled: LogicalPlan = null
    var planEnabled: LogicalPlan = null
    var expectedAnswer: Array[Row] = null

    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      planDisabled = sql(query).queryExecution.optimizedPlan
      expectedAnswer = sql(query).collect()
    }

    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      planEnabled = sql(query).queryExecution.optimizedPlan
      checkAnswer(sql(query), expectedAnswer)
      assert(getNumBloomFilters(planDisabled) == 0)
      if (shouldReplace) {
        assert(!columnPruningTakesEffect(planEnabled))
        assert(getNumBloomFilters(planEnabled) == runtimeFilterNum)
      } else {
        assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled))
      }
    }
  }

  def getNumBloomFilters(plan: LogicalPlan): Integer = {
    val numBloomFilterAggs = plan.collect {
      case Filter(condition, _) => condition.collect {
        case subquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery
        => subquery.plan.collect {
          case Aggregate(_, aggregateExpressions, _, _) =>
            aggregateExpressions.map {
              case Alias(AggregateExpression(bfAgg : BloomFilterAggregate, _, _, _, _),
              _) =>
                assert(bfAgg.estimatedNumItemsExpression.isInstanceOf[Literal])
                assert(bfAgg.numBitsExpression.isInstanceOf[Literal])
                1
            }.sum
        }.sum
      }.sum
    }.sum
    val numMightContains = plan.collect {
      case Filter(condition, _) => condition.collect {
        case BloomFilterMightContain(_, _) => 1
      }.sum
    }.sum
    assert(numBloomFilterAggs == numMightContains)
    numMightContains
  }

  def columnPruningTakesEffect(plan: LogicalPlan): Boolean = {
    def takesEffect(plan: LogicalPlan): Boolean = {
      val result = org.apache.spark.sql.catalyst.optimizer.ColumnPruning.apply(plan)
      !result.fastEquals(plan)
    }

    plan.collectFirst {
      case Filter(condition, _) if condition.collectFirst {
        case subquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery
          if takesEffect(subquery.plan) => true
      }.nonEmpty => true
    }.nonEmpty
  }

  def assertRewroteWithBloomFilter(query: String, runtimeFilterNum: Int = 1): Unit = {
    checkWithAndWithoutFeatureEnabled(query, shouldReplace = true, runtimeFilterNum)
  }

  def assertDidNotRewriteWithBloomFilter(query: String): Unit = {
    checkWithAndWithoutFeatureEnabled(query, shouldReplace = false)
  }

  // Runs `f` with the thresholds of the DPP tests and `confs`, with AQE on and off unless `confs`
  // sets it.
  def withDppTestConf(confs: (String, String)*)(f: => Unit): Unit = {
    val adaptiveModes = confs.collectFirst {
      case (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, adaptive) => Seq(adaptive)
    }.getOrElse(Seq("true", "false"))
    adaptiveModes.foreach { adaptive =>
      withSQLConf(Seq(
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive) ++ confs: _*)(f)
    }
  }

  // Runs `query` and returns the DPP filters that physical planning left on the file source scans:
  // the class of the subquery that each filter reads, or the filter itself if it reads none.
  def dynamicPruningFilters(query: String): Seq[Any] = {
    // Keep the scans in the final plan even if they return no rows.
    withSQLConf(
        SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      val df = sql(query)
      df.collect()
      collect(df.queryExecution.executedPlan) {
        case scan: FileSourceScanExec => scan.partitionFilters.collect {
          case DynamicPruningExpression(in: InSubqueryExec) => in.plan.getClass
          case DynamicPruningExpression(filter) => filter
        }
      }.flatten
    }
  }

  test("SPARK-58272: safely use fully materialized selectively filtered caches") {
    val cacheName = "cached_bloom_filter_keys"
    val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"

    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      withTempView(cacheName) {
        withCache(cacheName) {
          spark.range(0, 40, 1, numPartitions = 4)
            .where("id = 8")
            .selectExpr("CAST(id AS INT) AS c2")
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView(cacheName)

          def cachedRelation: InMemoryRelation = {
            spark.table(cacheName).queryExecution.withCachedData.collectFirst {
              case relation: InMemoryRelation => relation
            }.get
          }

          val before = cachedRelation
          assert(before.materializedMetadata.isEmpty)
          assert(!before.statsAvailable)
          assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)

          val buffers = before.cacheBuilder.cachedColumnBuffers
          assert(buffers.getNumPartitions > 1)
          spark.sparkContext.runJob(
            buffers,
            (iterator: Iterator[CachedBatch]) => iterator.size,
            Seq(0))
          assert(before.materializedMetadata.isEmpty)
          assert(!before.statsAvailable)
          assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)

          assert(spark.table(cacheName).count() == 1)
          val materialized = cachedRelation
          val metadata = materialized.materializedMetadata.get
          assert(metadata.rowCount == 1L)
          assert(metadata.statsAvailable)
          assert(materialized.statsAvailable)
          assert(materialized.isOutputRepeatable)
          assert(materialized.hasSelectivePredicate)
          val cachedSize = materialized.stats.sizeInBytes
          assert(cachedSize > 0)

          withSQLConf(
              SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key ->
                (cachedSize - 1).toString) {
            assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)
          }

          var expected: Array[Row] = null
          withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
            expected = sql(query).collect()
          }
          withSQLConf(
              SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key ->
                cachedSize.toString) {
            val actual = sql(query)
            assert(getNumBloomFilters(actual.queryExecution.optimizedPlan) == 1)
            checkAnswer(actual, expected)

            val leftOuter =
              s"SELECT * FROM bf1 LEFT OUTER JOIN $cacheName ON bf1.c1 = $cacheName.c2"
            assert(getNumBloomFilters(sql(leftOuter).queryExecution.optimizedPlan) == 0)
          }
        }
      }
    }
  }

  test("SPARK-58272: bound materialized caches by Bloom filter capacity") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.key -> "4",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      Seq(
        ("at_bloom_capacity", 4L, 1),
        ("over_bloom_capacity", 5L, 0)
      ).foreach { case (cacheName, rowCount, expectedBloomFilters) =>
        withTempView(cacheName) {
          withCache(cacheName) {
            spark.range(0, 8, 1, numPartitions = 4)
              .where(s"id < $rowCount")
              .selectExpr("CAST(id AS INT) AS c2")
              .persist(StorageLevel.MEMORY_AND_DISK)
              .createOrReplaceTempView(cacheName)
            assert(spark.table(cacheName).count() == rowCount)

            val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
              case cached: InMemoryRelation => cached
            }.get
            assert(relation.statsAvailable)
            assert(relation.hasSelectivePredicate)
            assert(relation.stats.rowCount.contains(rowCount))

            val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"
            assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) ==
              expectedBloomFilters)
          }
        }
      }
    }
  }

  test("SPARK-58272: size projected cached runtime filters using exact materialized rows") {
    val cacheName = "projected_cached_bloom_filter_keys"
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS.key -> "1",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.CBO_ENABLED.key -> "false") {
      withTempView(cacheName) {
        withCache(cacheName) {
          spark.range(0, 8, 1, numPartitions = 2)
            .where("id < 4")
            .selectExpr("CAST(id AS INT) AS c2", "CAST(id * 2 AS INT) AS payload")
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView(cacheName)
          assert(spark.table(cacheName).count() == 4)

          val cachedRelation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
            case relation: InMemoryRelation => relation
          }.get
          assert(cachedRelation.stats.rowCount.contains(BigInt(4)))
          assert(cachedRelation.hasSelectivePredicate)

          val projectedKeys = spark.table(cacheName).selectExpr("c2 + 1 AS projected_key")
          val projectedPlan = projectedKeys.queryExecution.optimizedPlan
          assert(projectedPlan.stats.rowCount.isEmpty)
          assert(projectedPlan.stats.sizeInBytes < cachedRelation.stats.sizeInBytes)

          val fact = spark.table("bf1")
          val query = fact.join(projectedKeys, fact("c1") === projectedKeys("projected_key"))
          val plan = withSQLConf(
              SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key ->
                projectedPlan.stats.sizeInBytes.toString) {
            query.queryExecution.optimizedPlan
          }
          assert(getNumBloomFilters(plan) == 1)

          val bloomAggregates = plan.collect {
            case Filter(condition, _) => condition.collect {
              case subquery: ScalarSubquery => subquery.plan.collect {
                case Aggregate(_, aggregateExpressions, _, _) => aggregateExpressions.collect {
                  case Alias(AggregateExpression(aggregate: BloomFilterAggregate, _, _, _, _), _) =>
                    aggregate
                }
              }.flatten
            }.flatten
          }.flatten
          assert(bloomAggregates.size == 1)
          assert(bloomAggregates.head.estimatedNumItemsExpression.eval() == 4L)
        }
      }
    }
  }

  test("SPARK-58272: preserve selective runtime filters over all cache states") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      Seq(
        ("memory_only_filtered_bloom_keys", StorageLevel.MEMORY_ONLY, true),
        ("cold_filtered_bloom_keys", StorageLevel.MEMORY_AND_DISK, false),
        ("materialized_filtered_bloom_keys", StorageLevel.MEMORY_AND_DISK, true)).foreach {
        case (cacheName, storageLevel, materialize) =>
          withTempView(cacheName) {
            withCache(cacheName) {
              val cached = spark.range(0, 40, 1, numPartitions = 4)
                .selectExpr("CAST(id AS INT) AS c2")
                .persist(storageLevel)
              cached.createOrReplaceTempView(cacheName)

              if (materialize) {
                assert(cached.count() == 40)
              }

              val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
                case cachedRelation: InMemoryRelation => cachedRelation
              }.get
              assert(relation.statsAvailable == (storageLevel.useDisk && materialize))
              assert(relation.isOutputRepeatable == materialize)

              val query = s"SELECT * FROM bf1 JOIN " +
                s"(SELECT * FROM $cacheName WHERE c2 = 8) filtered_keys " +
                "ON bf1.c1 = filtered_keys.c2"
              val actual = sql(query)
              assert(getNumBloomFilters(actual.queryExecution.optimizedPlan) == 1)

              var expected: Array[Row] = null
              withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
                expected = sql(query).collect()
              }
              checkAnswer(actual, expected)
            }
          }
      }
    }
  }

  test("SPARK-58272: reject materialized runtime filters for nonbinary collated join keys") {
    val cacheName = "collated_cached_bloom_filter_keys"
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      withTempView(cacheName) {
        withCache(cacheName) {
          spark.range(0, 40, 1, numPartitions = 4)
            .where("id = 1")
            .selectExpr("IF(id = 1, 'a', 'z') COLLATE UTF8_LCASE AS k")
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView(cacheName)
          assert(spark.table(cacheName).count() == 1)

          val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
            case cached: InMemoryRelation => cached
          }.get
          assert(relation.statsAvailable)
          assert(relation.hasSelectivePredicate)

          val actual = sql(
            s"SELECT fact.k FROM " +
              "(SELECT (CASE WHEN a1 = 73 THEN 'A' ELSE 'X' END) " +
              "COLLATE UTF8_LCASE AS k FROM bf1) fact " +
              s"JOIN $cacheName ON fact.k = $cacheName.k COLLATE UTF8_LCASE")
          assert(getNumBloomFilters(actual.queryExecution.optimizedPlan) == 0)
          checkAnswer(actual, Row("A"))
        }
      }
    }
  }

  test("SPARK-58272: use materialized caches without predicates only with pruning statistics") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.CBO_ENABLED.key -> "true") {
      val factPlan = spark.table("bf1").queryExecution.optimizedPlan
      val factKey = factPlan.output.find(_.name == "c1").get
      val applicationDistinctCount =
        factPlan.stats.attributeStats.get(factKey).flatMap(_.distinctCount).get
      assert(applicationDistinctCount > 5 && applicationDistinctCount < 100)

      Seq(("small_cached_bloom_keys", 8L, 13L, true),
        ("large_cached_bloom_keys", 0L, 100L, false)).foreach {
        case (cacheName, start, end, shouldInject) =>
          withTempView(cacheName) {
            withCache(cacheName) {
              spark.range(start, end, 1, numPartitions = 4)
                .selectExpr("CAST(id AS INT) AS c2")
                .persist(StorageLevel.MEMORY_AND_DISK)
                .createOrReplaceTempView(cacheName)
              spark.table(cacheName).count()

              val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
                case cached: InMemoryRelation => cached
              }.get
              assert(relation.statsAvailable)
              assert(!relation.hasSelectivePredicate)

              val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"
              val actual = sql(query)
              assert(getNumBloomFilters(actual.queryExecution.optimizedPlan) ==
                (if (shouldInject) 1 else 0))

              if (shouldInject) {
                val derivedJoin = sql(s"SELECT * FROM bf1 JOIN $cacheName " +
                  s"ON bf1.c1 % 2 = $cacheName.c2 % 2")
                assert(getNumBloomFilters(derivedJoin.queryExecution.optimizedPlan) == 0)

                val projectedFact = spark.table("bf1").selectExpr("c1 AS fact_key")
                val projectedKeys = spark.table(cacheName).selectExpr("c2 AS cached_key")
                val projectedJoin = projectedFact.join(
                  projectedKeys, projectedFact("fact_key") === projectedKeys("cached_key"))
                assert(getNumBloomFilters(projectedJoin.queryExecution.optimizedPlan) == 1)

                var expected: Array[Row] = null
                withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
                  expected = sql(query).collect()
                }
                checkAnswer(actual, expected)

                withSQLConf(SQLConf.CBO_ENABLED.key -> "false") {
                  assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)
                }
              }
            }
          }
      }
    }
  }

  test("SPARK-58272: use filtered application-side distinct counts") {
    val cacheName = "filtered_application_bloom_keys"
    val nullFilteredCacheName = "null_filtered_application_bloom_keys"

    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.CBO_ENABLED.key -> "true") {
      withTempView(cacheName, nullFilteredCacheName) {
        withCache(cacheName, nullFilteredCacheName) {
          spark.range(8, 13, 1, numPartitions = 4)
            .selectExpr("CAST(id AS INT) AS c2")
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView(cacheName)
          assert(spark.table(cacheName).count() == 5)

          val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
            case cached: InMemoryRelation => cached
          }.get
          assert(relation.statsAvailable)
          assert(!relation.hasSelectivePredicate)

          val filteredFact = spark.table("bf1").where("a1 = 77")
          val factPlan = filteredFact.queryExecution.optimizedPlan
          val factKey = factPlan.output.find(_.name == "c1").get
          assert(factPlan.stats.attributeStats.get(factKey)
            .flatMap(_.distinctCount).contains(BigInt(1)))

          Seq(
            s"SELECT * FROM (SELECT * FROM bf1 WHERE a1 = 77) fact " +
              s"JOIN $cacheName keys ON fact.c1 = keys.c2",
            s"SELECT * FROM $cacheName keys " +
              "JOIN (SELECT * FROM bf1 WHERE a1 = 77) fact ON keys.c2 = fact.c1"
          ).foreach { query =>
            var expected: Array[Row] = null
            withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
              expected = sql(query).collect()
            }

            val actual = sql(query)
            val plan = actual.queryExecution.optimizedPlan
            assert(getNumBloomFilters(plan) == 1)
            val applicationSides = plan.collect {
              case Filter(condition, child)
                  if condition.exists(_.isInstanceOf[BloomFilterMightContain]) => child
            }
            assert(applicationSides.size == 1)
            assert(applicationSides.head.exists(_.isInstanceOf[InMemoryRelation]), plan.treeString)
            checkAnswer(actual, expected)
          }

          val windowedQuery =
            "SELECT * FROM (" +
              "SELECT *, ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY a1) AS rn " +
              "FROM bf1 WHERE a1 = 77) fact " +
              s"JOIN $cacheName keys ON fact.c1 = keys.c2"
          assert(getNumBloomFilters(sql(windowedQuery).queryExecution.optimizedPlan) == 0)

          spark.range(0, 6, 1, numPartitions = 4)
            .selectExpr(
              "CAST(CASE id WHEN 0 THEN 0 WHEN 1 THEN 8 WHEN 2 THEN 23 " +
                "WHEN 3 THEN 58 WHEN 4 THEN 74 ELSE 86 END AS INT) AS c2")
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView(nullFilteredCacheName)
          assert(spark.table(nullFilteredCacheName).count() == 6)

          val nullFilteredFact = spark.table("bf1").where("a1 IS NOT NULL")
          val nullFilteredPlan = nullFilteredFact.queryExecution.optimizedPlan
          val nullFilteredKey = nullFilteredPlan.output.find(_.name == "c1").get
          assert(nullFilteredPlan.stats.attributeStats.get(nullFilteredKey)
            .flatMap(_.distinctCount).contains(BigInt(6)))

          val query = s"SELECT * FROM (SELECT * FROM bf1 WHERE a1 IS NOT NULL) fact " +
            s"JOIN $nullFilteredCacheName keys ON fact.c1 = keys.c2"
          assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)
        }
      }
    }
  }

  test("SPARK-58272: preserve transitive selective filters past materialized caches") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1MB",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.CBO_ENABLED.key -> "true") {
      val factPlan = spark.table("bf3").queryExecution.optimizedPlan
      val factKey = factPlan.output.find(_.name == "c3").get
      val applicationDistinctCount =
        factPlan.stats.attributeStats.get(factKey).flatMap(_.distinctCount).get
      assert(applicationDistinctCount > 5 && applicationDistinctCount < 100)

      Seq(
        ("large_transitive_bloom_keys", 0L, 100L, "1MB", false),
        ("oversized_transitive_bloom_keys", 8L, 13L, "0", false),
        ("selective_transitive_bloom_keys", 0L, 1000L, "1MB", true)
      ).foreach {
        case (cacheName, start, end, materializedThreshold, hasHiddenSelectivePredicate) =>
          withTempView(cacheName) {
            withCache(cacheName) {
              val keys = spark.range(start, end, 1, numPartitions = 4)
              val cachedKeys = if (hasHiddenSelectivePredicate) {
                keys.where("id < 100")
              } else {
                keys
              }
              cachedKeys
                .selectExpr("CAST(id AS INT) AS c2")
                .persist(StorageLevel.MEMORY_AND_DISK)
                .createOrReplaceTempView(cacheName)
              val expectedCount = if (hasHiddenSelectivePredicate) 100L else end - start
              assert(spark.table(cacheName).count() == expectedCount)

              val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
                case cached: InMemoryRelation => cached
              }.get
              assert(relation.statsAvailable)
              assert(relation.hasSelectivePredicate == hasHiddenSelectivePredicate)

              withSQLConf(
                  SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD.key ->
                    materializedThreshold) {
                Seq(
                  s"SELECT /*+ BROADCAST(selective) */ keys.c2 FROM $cacheName keys " +
                    "JOIN (SELECT c2 FROM bf2 WHERE a2 = 5) selective " +
                    "ON keys.c2 = selective.c2",
                  s"SELECT /*+ BROADCAST(selective) */ keys.c2 " +
                    "FROM (SELECT c2 FROM bf2 WHERE a2 = 5) selective " +
                    s"JOIN $cacheName keys ON selective.c2 = keys.c2"
                ).foreach { creationSide =>
                  val query = s"SELECT * FROM bf3 fact JOIN ($creationSide) creation " +
                    "ON fact.c3 = creation.c2"
                  var expected: Array[Row] = null
                  withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
                    expected = sql(query).collect()
                  }

                  val actual = sql(query)
                  val plan = actual.queryExecution.optimizedPlan
                  assert(getNumBloomFilters(plan) == 1)
                  val applicationSides = plan.collect {
                    case Filter(condition, child)
                        if condition.exists(_.isInstanceOf[BloomFilterMightContain]) => child
                  }
                  assert(applicationSides.size == 1)
                  assert(applicationSides.head.output.exists(_.name == "c3"), plan.treeString)
                  val creationPlans = plan.collect {
                    case Filter(condition, _) => condition.collect {
                      case BloomFilterMightContain(subquery: ScalarSubquery, _) => subquery.plan
                    }
                  }.flatten
                  assert(creationPlans.size == 1)
                  assert(!creationPlans.head.exists(_.isInstanceOf[InMemoryRelation]),
                    plan.treeString)
                  checkAnswer(actual, expected)
                }
              }
            }
          }
      }
    }
  }

  test("SPARK-58272: reject memory-only and non-repeatable materialized caches") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      Seq(("memory_only_bloom_keys", false),
        ("nondeterministic_bloom_keys", true)).foreach {
        case (cacheName, nondeterministic) =>
          withTempView(cacheName) {
            withCache(cacheName) {
              val keys = if (nondeterministic) {
                spark.range(0, 40, 1, numPartitions = 4)
                  .selectExpr("CAST(rand() * 20 AS INT) AS c2")
                  .where("c2 < 10")
                  .persist(StorageLevel.MEMORY_AND_DISK)
              } else {
                spark.range(0, 40, 1, numPartitions = 4)
                  .where("id = 8")
                  .selectExpr("CAST(id AS INT) AS c2")
                  .persist(StorageLevel.MEMORY_ONLY)
              }
              keys.createOrReplaceTempView(cacheName)
              keys.count()

              val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
                case cached: InMemoryRelation => cached
              }.get
              assert(!relation.statsAvailable)

              val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"
              assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)
            }
          }
      }
    }
  }

  test("SPARK-58272: do not trust cached runtime-replaceable encryption") {
    val cacheName = "encrypted_cached_bloom_filter_keys"
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
      withTempView(cacheName) {
        withCache(cacheName) {
          spark.range(0, 40, 1, numPartitions = 4)
            .where("id = 8")
            .selectExpr(
              "CAST(id AS INT) AS c2",
              "aes_encrypt(CAST(id AS STRING), '0000111122223333') AS encrypted")
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView(cacheName)
          assert(spark.table(cacheName).count() == 1)

          val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
            case cached: InMemoryRelation => cached
          }.get
          assert(relation.hasSelectivePredicate)
          assert(!relation.statsAvailable)

          val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"
          assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)
        }
      }
    }
  }

  test("SPARK-58272: use materialized selectively filtered file-backed caches") {
    val cacheName = "parquet_cached_bloom_filter_keys"
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.IGNORE_MISSING_FILES.key -> "false",
        SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
      withTempPath { path =>
        spark.range(0, 40, 1, numPartitions = 4).write.parquet(path.getCanonicalPath)
        withTempView(cacheName) {
          withCache(cacheName) {
            spark.read.parquet(path.getCanonicalPath)
              .where("id = 8")
              .selectExpr("CAST(id AS INT) AS c2")
              .persist(StorageLevel.MEMORY_AND_DISK)
              .createOrReplaceTempView(cacheName)
            assert(spark.table(cacheName).count() == 1)

            val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
              case cached: InMemoryRelation => cached
            }.get
            assert(relation.statsAvailable)
            assert(relation.hasSelectivePredicate)

            val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"
            assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 1)
          }
        }
      }
    }
  }

  test("SPARK-58272: materialized creation-side threshold exceeds broadcast threshold") {
    val conf = new SQLConf
    assert(conf.runtimeFilterMaterializedCreationSideThreshold > conf.autoBroadcastJoinThreshold)
  }

  test("Runtime bloom filter join: simple") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertRewroteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 " +
        "where bf2.a2 = 62")
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2")
    }
  }

  test("Runtime bloom filter join: two joins") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      // bf2 as creation side and inject runtime filter for bf1 and bf3.
      assertRewroteWithBloomFilter("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
        "and bf3.c3 = bf2.c2 where bf2.a2 = 5", 2)
      assertRewroteWithBloomFilter("select * from bf1 left outer join bf2 join bf3 on " +
        "bf1.c1 = bf2.c2 and bf3.c3 = bf2.c2 where bf2.a2 = 5", 2)
      assertRewroteWithBloomFilter("select * from bf1 right outer join bf2 join bf3 on " +
        "bf1.c1 = bf2.c2 and bf3.c3 = bf2.c2 where bf2.a2 = 5", 2)
      // bf1 and bf2 hasn't shuffle. bf1 as creation side and inject runtime filter for bf3.
      assertRewroteWithBloomFilter("select * from (select * from bf1 left semi join bf2 on " +
        "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1")
      assertRewroteWithBloomFilter("select * from (select * from bf1 left anti join bf2 on " +
        "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1")
      // bf1 as creation side and inject runtime filter for bf2 and bf3.
      assertRewroteWithBloomFilter("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
        "and bf3.c3 = bf1.c1 where bf1.a1 = 5", 2)
      assertRewroteWithBloomFilter("select * from bf1 left outer join bf2 join bf3 on " +
        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf1.a1 = 5", 2)
      assertRewroteWithBloomFilter("select * from bf1 right outer join bf2 join bf3 on " +
        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf1.a1 = 5", 2)
      // bf2 as creation side and inject runtime filter for bf1 and bf3(join keys are transitive).
      assertRewroteWithBloomFilter("select * from (select * from bf1 join bf2 on " +
        "bf1.c1 = bf2.c2 where bf2.a2 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
      assertRewroteWithBloomFilter("select * from (select * from bf1 left join bf2 on " +
        "bf1.c1 = bf2.c2 where bf2.a2 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
      assertRewroteWithBloomFilter("select * from (select * from bf1 right join bf2 on " +
        "bf1.c1 = bf2.c2 where bf2.a2 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
      assertRewroteWithBloomFilter("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
        "and bf3.c3 = bf1.c1 where bf2.a2 = 5", 2)
      assertRewroteWithBloomFilter("select * from bf1 left outer join bf2 join bf3 on " +
        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf2.a2 = 5", 2)
      assertRewroteWithBloomFilter("select * from bf1 right outer join bf2 join bf3 on " +
        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf2.a2 = 5", 2)
    }

    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1200") {
      // bf1 as creation side and inject runtime filter for bf2 and bf3.
      assertRewroteWithBloomFilter("select * from (select * from bf1 left semi join bf2 on " +
        "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
      // left anti join unsupported. bf1 as creation side and inject runtime filter for bf3.
      assertRewroteWithBloomFilter("select * from (select * from bf1 left anti join bf2 on " +
        "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1")
      // bf2 as creation side and inject runtime filter for bf1 and bf3(by passing key).
      assertRewroteWithBloomFilter("select * from (select * from bf1 left semi join bf2 on " +
        "(bf1.c1 = bf2.c2 and bf2.a2 = 5)) as a join bf3 on bf3.c3 = a.c1", 2)
      // left anti join unsupported.
      // bf2 as creation side and inject runtime filter for bf3(by passing key).
      assertDidNotRewriteWithBloomFilter("select * from (select * from bf1 left anti join bf2 " +
        "on (bf1.c1 = bf2.c2 and bf2.a2 = 5)) as a join bf3 on bf3.c3 = a.c1")
      // left anti join unsupported and hasn't selective filter.
      assertRewroteWithBloomFilter("select * from (select * from bf1 left anti join bf2 on " +
        "(bf1.c1 = bf2.c2 and bf1.a1 = 5)) as a join bf3 on bf3.c3 = a.c1", 0)
    }
  }

  test("Runtime bloom filter join: two filters single join") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      var planDisabled: LogicalPlan = null
      var planEnabled: LogicalPlan = null
      var expectedAnswer: Array[Row] = null

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.b1 = bf2.b2 where bf2.a2 = 62"

      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planDisabled = sql(query).queryExecution.optimizedPlan
        expectedAnswer = sql(query).collect()
      }

      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
        planEnabled = sql(query).queryExecution.optimizedPlan
        checkAnswer(sql(query), expectedAnswer)
      }
      assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled) + 2)
    }
  }

  test("Runtime bloom filter join: test the number of filter threshold") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      var planDisabled: LogicalPlan = null
      var planEnabled: LogicalPlan = null
      var expectedAnswer: Array[Row] = null

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.b1 = bf2.b2 where bf2.a2 = 62"

      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planDisabled = sql(query).queryExecution.optimizedPlan
        expectedAnswer = sql(query).collect()
      }

      for (numFilterThreshold <- 0 to 3) {
        withSQLConf( SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
          SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD.key -> numFilterThreshold.toString) {
          planEnabled = sql(query).queryExecution.optimizedPlan
          checkAnswer(sql(query), expectedAnswer)
        }
        if (numFilterThreshold < 3) {
          assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled)
            + numFilterThreshold)
        } else {
          assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled) + 2)
        }
      }
    }
  }

  test("Runtime bloom filter join: insert one bloom filter per column") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      var planDisabled: LogicalPlan = null
      var planEnabled: LogicalPlan = null
      var expectedAnswer: Array[Row] = null

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.c1 = bf2.b2 where bf2.a2 = 62"

      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planDisabled = sql(query).queryExecution.optimizedPlan
        expectedAnswer = sql(query).collect()
      }

      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
        planEnabled = sql(query).queryExecution.optimizedPlan
        checkAnswer(sql(query), expectedAnswer)
      }
      assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled) + 1)
    }
  }

  test("Runtime bloom filter join: do not add bloom filter if dpp filter exists " +
    "on the same column") {
    withDppTestConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
      val query = "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 where bf2.a2 = 62"
      assertDidNotRewriteWithBloomFilter(query)
      assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryExec]))
    }
  }

  test("Runtime bloom filter join: add bloom filter if dpp filter exists on " +
    "a different column") {
    withDppTestConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
      val query = "select * from bf5part join bf2 on " +
        "bf5part.c5 = bf2.c2 and bf5part.f5 = bf2.f2 where bf2.a2 = 62"
      assertRewroteWithBloomFilter(query)
      assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryExec]))
    }
  }

  test("SPARK-60702: add bloom filter if the dpp filter needs a broadcast that the shuffle " +
    "join lacks") {
    Seq(
      "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 where bf2.a2 = 62",
      "select * from bf2 join bf5part on bf2.c2 = bf5part.f5 where bf2.a2 = 62").foreach { query =>
      withDppTestConf() {
        assertRewroteWithBloomFilter(query)
        assert(dynamicPruningFilters(query) == Seq(Literal.TrueLiteral))
      }
    }
  }

  test("SPARK-60702: do not add bloom filter if the dpp filter reuses the broadcast of " +
    "the join") {
    // The aggregate shuffles below the broadcast join, so the join may get a bloom filter. The
    // join broadcasts `bf2` by a hint, with the partitioned table on either side, or by its size.
    // Without DPP, or without exchange reuse to give the DPP filter the broadcast, the join gets
    // the bloom filter.
    Seq(
      "select /*+ broadcast(bf2) */ * from (select f5, sum(a5) from bf5part group by f5) t " +
        "join bf2 on t.f5 = bf2.c2 where bf2.a2 = 62" -> "2000",
      "select /*+ broadcast(bf2) */ * from bf2 join (select f5, sum(a5) from bf5part " +
        "group by f5) t on bf2.c2 = t.f5 where bf2.a2 = 62" -> "2000",
      "select * from (select f5, sum(a5) from bf5part group by f5) t " +
        "join bf2 on t.f5 = bf2.c2 where bf2.a2 = 62" -> "5000"
    ).foreach { case (query, threshold) =>
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold) {
        withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
          assertRewroteWithBloomFilter(query)
        }
        withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
          assertRewroteWithBloomFilter(query)
          assert(dynamicPruningFilters(query) == Seq(Literal.TrueLiteral))
        }
        assertDidNotRewriteWithBloomFilter(query)
        assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryBroadcastExec]))
      }
    }
  }

  test("SPARK-60702: do not add bloom filter if another join broadcasts the build plan of " +
    "the dpp filter") {
    // The first join is a shuffle join, and its DPP filter reuses the broadcast of the second.
    val query = "select p.f5, p.a5, d.* from bf5part p join bf2 d on p.f5 = d.c2 " +
      "where d.a2 = 62 union all select /*+ broadcast(d) */ t.f5, t.s, d.* from " +
      "(select f5, sum(a5) s from bf5part group by f5) t join bf2 d on t.f5 = d.c2 " +
      "where d.a2 = 62"
    withDppTestConf() {
      assertDidNotRewriteWithBloomFilter(query)
      // One of the DPP filters can read the broadcast through a reused subquery.
      val reuses = Set[Any](classOf[SubqueryBroadcastExec], classOf[ReusedSubqueryExec])
      val filters = dynamicPruningFilters(query)
      assert(filters.size == 2 && filters.forall(reuses.contains), filters)
    }
  }

  test("SPARK-60702: do not add bloom filter that takes a broadcast from a dpp filter") {
    // The DPP filter on `q.f5` cannot reuse a broadcast, so it does not keep the bloom filter off
    // its key. That bloom filter would go into `r`, the broadcast that the DPP filter on `p.f5`
    // reuses.
    val query = "select /*+ broadcast(r) */ * from bf5part p join (select q.f5 rf, q.a5 ra from " +
      "bf5filtered q join bf2 on q.f5 = bf2.c2 where bf2.a2 = 62) r on p.f5 = r.rf"
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
        assertRewroteWithBloomFilter(query)
      }
      assertDidNotRewriteWithBloomFilter(query)
      assert(dynamicPruningFilters(query).contains(classOf[SubqueryBroadcastExec]))
    }
  }

  test("SPARK-60702: do not add bloom filter that takes a broadcast from a dpp filter in " +
    "another union branch") {
    // The DPP filter on `u.k` cannot reuse a broadcast, so it does not keep the bloom filter off
    // its key. Pushdown would move that bloom filter into both branches, and so into `d`, the
    // broadcast that the DPP filter on `q.f5` reuses. Over the cache of `d`, cleanup drops the
    // copy of the DPP filter on `u.k`, but not the bloom filter.
    val query = "select * from (select p.f5 k, p.a5 v from bf5part p union all select " +
      "/*+ broadcast(d) */ d.c2 k, q.a5 v from bf5part q join bf2c d on q.f5 = d.c2 " +
      "where d.a2 = 62) u join bf3 on u.k = bf3.c3 where bf3.a3 = 62"
    withTempView("bf2c") {
      withCache("bf2c") {
        spark.table("bf2").createOrReplaceTempView("bf2c")
        spark.catalog.cacheTable("bf2c")
        withDppTestConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          assertDidNotRewriteWithBloomFilter(query)
          assert(dynamicPruningFilters(query).contains(classOf[SubqueryBroadcastExec]))
        }
      }
    }
  }

  test("SPARK-60702: keep the bloom filter off a key whose dpp filter reuses a broadcast side " +
    "that has a dpp filter of its own") {
    // The DPP filter on `q.f5` sits on top of `q` when the rule runs, and cleanup drops it later
    // because of `q.f5 = 51`. Then the DPP filter on `t.f5` reuses the broadcast of `q`, so `t.f5`
    // gets no bloom filter. `t.c5` gets one, since the DPP filter on its key is gone.
    val query = "select /*+ broadcast(q) */ * from (select f5, c5, sum(a5) s from bf5part " +
      "where b5 > 0 group by f5, c5) t join bf5filtered q on t.f5 = q.c5 and t.c5 = q.f5 " +
      "where q.a5 > 50 and q.f5 = 51"
    withDppTestConf() {
      assertRewroteWithBloomFilter(query)
      assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryBroadcastExec]))
    }
  }

  test("SPARK-60702: do not add bloom filter that takes a broadcast from a dpp filter that is " +
    "not only in broadcast") {
    // With `reuseBroadcastOnly` off, the DPP filter on `p.f5` has a benefit, so it can run as its
    // own subquery, but physical planning reuses the broadcast of `r` first. The DPP filter on
    // `q.f5` has none, so it is only in broadcast and cannot reuse a broadcast. A bloom filter on
    // `q.f5` would go into `r`.
    withTable("bf5big") {
      sql("select a5 + id * 100 as a5, b5, c5, d5, e5, f5 from bf5part cross join range(400)")
        .write.partitionBy("f5").saveAsTable("bf5big")
      val query = "select /*+ broadcast(r) */ * from bf5big p join (select q.f5 rf, q.a5 ra " +
        "from bf5filtered q join bf5part d on q.f5 = d.c5 where d.a5 = 62) r on p.f5 = r.rf"
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
          SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.0",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        assertDidNotRewriteWithBloomFilter(query)
        assert(dynamicPruningFilters(query).contains(classOf[SubqueryBroadcastExec]))
      }
    }
  }

  test("SPARK-60702: keep the bloom filter of a key without a dpp filter") {
    // Pushdown moves the bloom filter on `s.k` into `x`, the broadcast that the DPP filter on
    // `p.f5` reuses. The key has no DPP filter, so the rule adds the bloom filter all the same.
    val query = "select * from (select x.c2 k, count(*) from bf5part p join (select c2 from bf2 " +
      "where a2 = 62) x on p.f5 = x.c2 group by x.c2) s join bf3 on s.k = bf3.c3 " +
      "where bf3.a3 = 62"
    withDppTestConf() {
      assertRewroteWithBloomFilter(query)
    }
  }

  test("SPARK-60702: do not add bloom filter if physical planning keeps the dpp filter over the " +
    "build plan of a null-aware anti join") {
    // The join is a shuffle join. The `not in` subquery broadcasts the build plan of its DPP
    // filter. Without AQE, physical planning keeps the filter, though it builds its own
    // broadcast. AQE plans the filter as `true`, so there the key gets the bloom filter.
    val query = "select * from bf5part p join (select c2 from bf2 where a2 = 62 and c2 is not " +
      "null) d on p.f5 = d.c2 where p.a5 not in (select c2 from bf2 where a2 = 62 and c2 is not " +
      "null)"
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500",
        SQLConf.NULL_AWARE_ANTI_JOIN_BROADCAST_THRESHOLD.key -> "-1") {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
        assertRewroteWithBloomFilter(query)
      }
      if (conf.adaptiveExecutionEnabled) {
        assertRewroteWithBloomFilter(query)
        assert(dynamicPruningFilters(query) == Seq(Literal.TrueLiteral))
      } else {
        assertDidNotRewriteWithBloomFilter(query)
        assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryBroadcastExec]))
      }
    }
  }

  test("SPARK-60702: add bloom filter if the join type rules out the broadcast the dpp filter " +
    "needs") {
    // `bf2` is small enough to broadcast, but a right outer join cannot broadcast its right side.
    val query = "select * from (select f5, sum(a5) s from bf5part group by f5) t " +
      "right join bf2 on t.f5 = bf2.c2 where bf2.a2 = 62"
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000") {
      assertRewroteWithBloomFilter(query)
      assert(dynamicPruningFilters(query) == Seq(Literal.TrueLiteral))
    }
  }

  test("SPARK-60702: add bloom filter if the broadcast join builds the side the dpp filter " +
    "prunes") {
    val query = "select /*+ broadcast(t) */ * from " +
      "(select f5, sum(a5) from bf5part group by f5) t join bf2 on t.f5 = bf2.c2 " +
      "where bf2.a2 = 62"
    withDppTestConf() {
      assertRewroteWithBloomFilter(query)
      assert(dynamicPruningFilters(query) == Seq(Literal.TrueLiteral))
    }
  }

  test("SPARK-60702: do not add bloom filter if the dpp filter can project its values from " +
    "a broadcast") {
    def query(broadcast: String): String = "select * from bf5part join " +
      s"(select /*+ broadcast($broadcast) */ bf3.f3 from bf2 join bf3 on bf2.c2 = bf3.c3 " +
      "where bf3.a3 = 62) t on bf5part.f5 = t.f3"
    val projection = SQLConf.DYNAMIC_PARTITION_PRUNING_BROADCAST_PROJECTION_ENABLED.key -> "true"
    withDppTestConf() {
      assertRewroteWithBloomFilter(query("bf3"))
    }
    // Nothing can broadcast `bf3`, so there is no broadcast to project from.
    withDppTestConf(projection, SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assertRewroteWithBloomFilter(query("bf2"))
      assert(dynamicPruningFilters(query("bf2")) == Seq(Literal.TrueLiteral))
    }
    withDppTestConf(projection, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      assertDidNotRewriteWithBloomFilter(query("bf3"))
      assert(dynamicPruningFilters(query("bf3")) ==
        Seq(classOf[ProjectedBroadcastValueSubqueryExec]))
    }
    // AQE plans the projected filter as `true`, see SPARK-60703.
    withDppTestConf(projection, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      assertDidNotRewriteWithBloomFilter(query("bf3"))
      assert(dynamicPruningFilters(query("bf3")) == Seq(Literal.TrueLiteral))
    }
  }

  test("Runtime bloom filter join: BF rewrite triggering threshold test") {
    // Filter creation side data size is 3409 bytes. On the filter application side, an individual
    // scan's byte size is 3362.
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3000",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "4000"
    ) {
      assertRewroteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 " +
        "where bf2.a2 = 62")
    }
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "50",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "50"
    ) {
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 " +
        "where bf2.a2 = 62")
    }
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "5000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3000",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "4000"
    ) {
      // Rewrite should not be triggered as the Bloom filter application side scan size is small.
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 "
        + "where bf2.a2 = 62")
    }
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "32",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "4000") {
      // Test that the max scan size rather than an individual scan size on the filter
      // application side matters. `bf5filtered` has 15049 bytes and `bf2` has 3409 bytes.
      withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "5000") {
        assertRewroteWithBloomFilter("select * from " +
          "(select * from bf5filtered union all select * from bf2) t " +
          "join bf3 on t.c5 = bf3.c3 where bf3.a3 = 5", 2)
      }
      withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "16000") {
        assertDidNotRewriteWithBloomFilter("select * from " +
          "(select * from bf5filtered union all select * from bf2) t " +
          "join bf3 on t.c5 = bf3.c3 where bf3.a3 = 5")
      }
    }
  }

  test("Runtime bloom filter join: simple expressions only") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      val squared = (s: Long) => {
        s * s
      }
      spark.udf.register("square", squared)
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on " +
        "bf1.c1 = bf2.c2 where square(bf2.a2) = 62" )
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on " +
        "bf1.c1 = square(bf2.c2) where bf2.a2 = 62" )
    }
  }

  test("SPARK-58310: skip runtime bloom filter when pruned creation side contains Python UDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)
    registerTestUDF(TestPythonUDF("normalize_runtime_bloom_url"), spark)
    val pythonUdfNames = if (shouldTestPandasUDFs) {
      registerTestUDF(TestScalarPandasUDF("normalize_runtime_bloom_pandas_url"), spark)
      Seq("normalize_runtime_bloom_url", "normalize_runtime_bloom_pandas_url")
    } else {
      Seq("normalize_runtime_bloom_url")
    }

    withTempPath { dir =>
      val factPath = new File(dir, "fact").getCanonicalPath
      val dimensionPath = new File(dir, "dimension").getCanonicalPath

      spark.range(20000)
        .selectExpr("cast(id as int) as id", "concat('url-', cast(id % 20 as string)) as url")
        .write.parquet(factPath)
      spark.range(20)
        .selectExpr(
          "concat('url-', cast(id as string)) as url",
          "case when id < 10 then 'NL' else 'US' end as country")
        .write.parquet(dimensionPath)

      withTempView("runtime_bloom_fact", "runtime_bloom_dimension") {
        spark.read.parquet(factPath).createOrReplaceTempView("runtime_bloom_fact")
        spark.read.parquet(dimensionPath).createOrReplaceTempView("runtime_bloom_dimension")

        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
          SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "100MB",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          pythonUdfNames.foreach { pythonUdfName =>
            withClue(s"$pythonUdfName: ") {
              val query =
                s"""
                  |WITH coverage AS (
                  |  SELECT url, $pythonUdfName(url) AS normalized_url
                  |  FROM runtime_bloom_dimension
                  |  WHERE country = 'NL'
                  |),
                  |grader_keys AS (
                  |  SELECT DISTINCT normalized_url
                  |  FROM coverage
                  |  WHERE normalized_url IS NOT NULL
                  |),
                  |matched_url_status AS (
                  |  SELECT /*+ BROADCAST(k) */ s.url
                  |  FROM runtime_bloom_fact s
                  |  JOIN grader_keys k ON s.url = k.normalized_url
                  |),
                  |url_status_snapshot AS (
                  |  SELECT max(url) AS snapshot_url FROM runtime_bloom_fact
                  |)
                  |SELECT /*+ BROADCAST(m), BROADCAST(snapshot) */
                  |  c.url, c.normalized_url, m.url AS matched_url, snapshot.snapshot_url
                  |FROM coverage c
                  |LEFT JOIN matched_url_status m ON c.normalized_url = m.url
                  |CROSS JOIN url_status_snapshot snapshot
                """.stripMargin

              val applicationSideQuery =
                s"""
                  |SELECT /*+ MERGE(f, s) */ f.id, f.url
                  |FROM (
                  |  SELECT id, url, $pythonUdfName(url) AS normalized_url
                  |  FROM runtime_bloom_fact
                  |) f
                  |JOIN (
                  |  SELECT url FROM runtime_bloom_dimension WHERE country = 'NL'
                  |) s ON f.normalized_url = s.url
                """.stripMargin

              val creationSideQuery =
                s"""
                  |SELECT /*+ MERGE(f, s) */ f.id, s.url, s.normalized_url
                  |FROM runtime_bloom_fact f
                  |JOIN (
                  |  SELECT url, $pythonUdfName(url) AS normalized_url
                  |  FROM runtime_bloom_dimension
                  |  WHERE country = 'NL'
                  |) s ON f.url = s.url
                """.stripMargin

              var expected: Array[Row] = null
              var applicationSideExpected: Array[Row] = null
              var creationSideExpected: Array[Row] = null
              withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
                expected = sql(query).collect()
                applicationSideExpected = sql(applicationSideQuery).collect()
                creationSideExpected = sql(creationSideQuery).collect()
              }

              assert(expected.length == 10000)
              assert(applicationSideExpected.length == 10000)
              assert(creationSideExpected.length == 10000)

              withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
                val result = sql(query)
                checkAnswer(result, expected.toSeq)

                val outputPath = new java.io.File(dir, pythonUdfName).getCanonicalPath
                result.write.parquet(outputPath)
                checkAnswer(spark.read.parquet(outputPath), expected.toSeq)

                val applicationSideResult = sql(applicationSideQuery)
                assert(getNumBloomFilters(applicationSideResult.queryExecution.optimizedPlan) > 0)
                checkAnswer(applicationSideResult, applicationSideExpected.toSeq)

                val creationSideResult = sql(creationSideQuery)
                assert(getNumBloomFilters(creationSideResult.queryExecution.optimizedPlan) > 0)
                checkAnswer(creationSideResult, creationSideExpected.toSeq)
              }
            }
          }
        }
      }
    }
  }

  test("Support Left Semi join in row level runtime filters") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "32") {
      assertRewroteWithBloomFilter(
        """
          |SELECT *
          |FROM   bf1 LEFT SEMI
          |JOIN   (SELECT * FROM bf2 WHERE bf2.a2 = 62) tmp
          |ON     bf1.c1 = tmp.c2
        """.stripMargin)
    }
  }

  test("Runtime Filter supports pruning side has Aggregate") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000") {
      assertRewroteWithBloomFilter(
        """
          |SELECT *
          |FROM   (SELECT c1 AS aliased_c1, d1 FROM bf1 GROUP BY c1, d1) bf1
          |       JOIN bf2 ON bf1.aliased_c1 = bf2.c2
          |WHERE  bf2.a2 = 62
        """.stripMargin)
    }
  }

  test("Runtime Filter supports pruning side has Window") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000") {
      assertRewroteWithBloomFilter(
        """
          |SELECT *
          |FROM   (SELECT *,
          |               Row_number() OVER (PARTITION BY c1 ORDER BY f1) rn
          |        FROM   bf1) bf1
          |       JOIN bf2 ON bf1.c1 = bf2.c2
          |WHERE  bf2.a2 = 62
        """.stripMargin)
    }
  }

  test("Merge runtime bloom filters") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      // Re-enable `MergeScalarSubqueries`
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.b1 = bf2.b2 where bf2.a2 = 62"
      val df = sql(query)
      df.collect()
      val plan = df.queryExecution.executedPlan

      val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
      val reusedSubqueryIds = collectWithSubqueries(plan) {
        case rs: ReusedSubqueryExec => rs.child.id
      }

      assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
      assert(reusedSubqueryIds.size == 1,
        "Missing or unexpected reused ReusedSubqueryExec in the plan")
    }
  }
}
