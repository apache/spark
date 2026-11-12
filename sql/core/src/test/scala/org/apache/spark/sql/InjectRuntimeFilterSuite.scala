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

import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, DynamicPruningExpression, DynamicPruningSubquery, Expression, Literal, ScalarSubquery, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.connector.StorageFilterTestSource
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, ProjectedBroadcastValueSubqueryExec, ReusedSubqueryExec, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEPropagateEmptyRelation}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelationWithTable
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

  // Runs `f` with the thresholds of the DPP tests, `reuseBroadcastOnly` on, and `confs`, with AQE
  // on and off unless `confs` sets it.
  def withDppTestConf(confs: (String, String)*)(f: => Unit): Unit = {
    val adaptiveModes = confs.collectFirst {
      case (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, adaptive) => Seq(adaptive)
    }.getOrElse(Seq("true", "false"))
    adaptiveModes.foreach { adaptive =>
      withSQLConf(Seq(
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive) ++ confs: _*) {
        withClue(s"adaptive=$adaptive: ")(f)
      }
    }
  }

  // Returns the DPP filters that physical planning left on the file source scans of `query`: the
  // class of the subquery that each filter reads, or the filter itself if it reads none. Only AQE
  // plans them as the query runs, so only then does it run the query.
  def dynamicPruningFilters(query: String): Seq[Any] = {
    // Keep the scans in the final plan even if they return no rows.
    withSQLConf(
        SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      val df = sql(query)
      if (conf.adaptiveExecutionEnabled) df.collect()
      collect(df.queryExecution.executedPlan) {
        case scan: FileSourceScanExec => scan.partitionFilters.collect {
          case DynamicPruningExpression(in: InSubqueryExec) => in.plan.getClass
          case DynamicPruningExpression(filter) => filter
        }
      }.flatten
    }
  }

  // Asserts that `query` gets a bloom filter, and that physical planning plans its DPP filter as
  // `true`.
  def assertRewroteWithBloomFilterForDroppedDpp(query: String): Unit = {
    assertRewroteWithBloomFilter(query)
    assert(dynamicPruningFilters(query) == Seq(Literal.TrueLiteral))
  }

  // The keys that the bloom filters of `query` check.
  def bloomFilterKeys(query: String): Seq[Expression] =
    sql(query).queryExecution.optimizedPlan.flatMap(_.expressions)
      .flatMap(_.collect { case BloomFilterMightContain(_, XxHash64(Seq(key), _)) => key })

  // The names of the columns that the bloom filters of `query` check.
  def bloomFilterKeyNames(query: String): Seq[String] =
    bloomFilterKeys(query).flatMap(_.references.map(_.name))

  // Asserts that `query` gets `numBloomFilters` bloom filters without DPP, so that a test that sees
  // fewer with DPP shows that the rule kept them out. In the tests for a key or a creation side
  // that is not safe, that is master's bloom filter for a key without a DPP filter, which can fail
  // the query or drop rows there: SPARK-58486 (a collated key), SPARK-60808 (a creation side that
  // can give other rows again), SPARK-60809 (a key that can fail) and SPARK-60842 (a creation side
  // that can fail on rows that only the join's side drops).
  def assertBloomFilterWithoutDpp(query: String, numBloomFilters: Int = 1): Unit =
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
      assert(bloomFilterKeys(query).size == numBloomFilters)
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

  test("SPARK-60871: no runtime filters for nonbinary collated join keys") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1MB") {
      withTable("bf_variant_fact", "bf_variant_dim") {
        sql("CREATE TABLE bf_variant_fact (id INT, v VARIANT) USING parquet")
        sql("""INSERT INTO bf_variant_fact SELECT id, parse_json(j) FROM VALUES
              |  (1, '{"k": "Open"}'), (2, '{"k": "x"}') AS t(id, j)""".stripMargin)
        sql("CREATE TABLE bf_variant_dim (id INT, v VARIANT) USING parquet")
        sql("""INSERT INTO bf_variant_dim SELECT id, parse_json(j) FROM VALUES
              |  (10, '{"k": "open"}'), (20, '{"k": "y"}') AS t(id, j)""".stripMargin)

        val dim = "(SELECT * FROM bf_variant_dim WHERE id = 10) d"
        def queries(targetType: String): Seq[String] = {
          val on = s"ON variant_get(f.v, '$$.k', '$targetType') = " +
            s"variant_get(d.v, '$$.k', '$targetType')"
          Seq(
            s"SELECT f.id FROM bf_variant_fact f JOIN $dim $on",
            s"SELECT f.id FROM bf_variant_fact f LEFT SEMI JOIN $dim $on",
            s"SELECT f.id FROM bf_variant_fact f RIGHT OUTER JOIN $dim $on",
            s"SELECT f.id FROM $dim LEFT OUTER JOIN bf_variant_fact f $on")
        }

        // Binary keys get a Bloom filter on the fact side.
        queries("STRING").foreach(assertRewroteWithBloomFilter(_))
        // A Bloom filter over UTF8_LCASE keys would drop the fact row 'Open', which the join
        // matches with 'open'.
        queries("STRING COLLATE UTF8_LCASE").foreach { query =>
          assertDidNotRewriteWithBloomFilter(query)
          checkAnswer(sql(query), Row(1))
        }
      }
    }
  }

  test("SPARK-60871: runtime filters for casts of collated join key columns") {
    withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1MB") {
      withTable("bf_cast_fact", "bf_cast_dim") {
        Seq("bf_cast_fact", "bf_cast_dim").foreach { t =>
          sql(s"CREATE TABLE $t (id INT, k STRING COLLATE UTF8_LCASE, p STRING) USING parquet")
        }
        sql("INSERT INTO bf_cast_fact VALUES (1, 'Open', 'Open'), (2, 'x', 'x')")
        sql("INSERT INTO bf_cast_dim VALUES (10, 'open', 'open'), (20, 'y', 'y')")

        val dim = "(SELECT * FROM bf_cast_dim WHERE id = 10) d"
        // Inner, left semi, right outer and left outer joins with the fact table as the
        // application side.
        def queries(key: String => String): Seq[String] = {
          val on = s"ON ${key("f")} = ${key("d")}"
          Seq(
            s"SELECT f.id FROM bf_cast_fact f JOIN $dim $on",
            s"SELECT f.id FROM bf_cast_fact f LEFT SEMI JOIN $dim $on",
            s"SELECT f.id FROM bf_cast_fact f RIGHT OUTER JOIN $dim $on",
            s"SELECT f.id FROM $dim LEFT OUTER JOIN bf_cast_fact f $on")
        }
        val matched = Seq.fill(4)(Seq(Row(1)))
        val unmatched = Seq(Seq.empty, Seq.empty, Seq(Row(null)), Seq(Row(null)))

        // Column keys get a Bloom filter on their collation keys.
        queries(t => s"$t.k").zip(matched).foreach { case (query, answer) =>
          assertRewroteWithBloomFilter(query)
          checkAnswer(sql(query), answer)
        }
        // Casts of the UTF8_LCASE column to STRING are binary keys, so 'Open' and 'open' differ.
        Seq[String => String](t => s"CAST($t.k AS STRING)", t => s"$t.k::STRING").foreach { key =>
          queries(key).zip(unmatched).foreach { case (query, answer) =>
            assertRewroteWithBloomFilter(query)
            checkAnswer(sql(query), answer)
          }
        }
        // Casts to UTF8_LCASE are non-binary expression keys, so they get no Bloom filter.
        Seq[String => String](
            t => s"CAST($t.p AS STRING COLLATE UTF8_LCASE)",
            t => s"$t.p::STRING COLLATE UTF8_LCASE").foreach { key =>
          queries(key).zip(matched).foreach { case (query, answer) =>
            assertDidNotRewriteWithBloomFilter(query)
            checkAnswer(sql(query), answer)
          }
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
      assert(bloomFilterKeys(query).isEmpty)
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
    // `bf2.a2 = 36` keeps a row of `bf2` that joins, so the bloom filter is not empty when it meets
    // the partition values of `bf5part`.
    Seq(
      "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 where bf2.a2 = 36",
      "select * from bf2 join bf5part on bf2.c2 = bf5part.f5 where bf2.a2 = 36").foreach { query =>
      withDppTestConf() {
        assertRewroteWithBloomFilterForDroppedDpp(query)
        assert(sql(query).collect().nonEmpty)
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
        assertBloomFilterWithoutDpp(query)
        withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
          assertRewroteWithBloomFilterForDroppedDpp(query)
        }
        assert(bloomFilterKeys(query).isEmpty)
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
      // Without DPP, both `p.f5` and `t.f5` get a bloom filter.
      assertBloomFilterWithoutDpp(query, 2)
      assert(bloomFilterKeys(query).isEmpty)
      // One of the DPP filters can read the broadcast through a reused subquery.
      val reuses = Set[Any](classOf[SubqueryBroadcastExec], classOf[ReusedSubqueryExec])
      val filters = dynamicPruningFilters(query)
      assert(filters.size == 2 && filters.forall(reuses.contains), filters)
    }
  }

  test("SPARK-60702: do not add bloom filter that takes a broadcast from a dpp filter") {
    // The DPP filter on `q.f5` cannot reuse a broadcast, so it does not keep the bloom filter off
    // its key. That bloom filter would go into `r`, the broadcast that the DPP filter on `p.f5`
    // reuses. `q` is on either side of its join.
    Seq(
      "select /*+ broadcast(r) */ * from bf5part p join (select q.f5 rf, q.a5 ra from " +
        "bf5filtered q join bf2 on q.f5 = bf2.c2 where bf2.a2 = 62) r on p.f5 = r.rf",
      "select /*+ broadcast(r) */ * from bf5part p join (select q.f5 rf, q.a5 ra from " +
        "bf2 join bf5filtered q on bf2.c2 = q.f5 where bf2.a2 = 62) r on p.f5 = r.rf"
    ).foreach { query =>
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        assertBloomFilterWithoutDpp(query)
        assert(bloomFilterKeys(query).isEmpty)
        assert(dynamicPruningFilters(query).contains(classOf[SubqueryBroadcastExec]))
      }
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
          // Without DPP, pushdown puts a bloom filter into each branch.
          assertBloomFilterWithoutDpp(query, 2)
          assert(bloomFilterKeys(query).isEmpty)
          assert(dynamicPruningFilters(query).contains(classOf[SubqueryBroadcastExec]))
        }
      }
    }
  }

  test("SPARK-60702: keep the bloom filter off a key whose dpp filter reuses a broadcast side " +
    "that has a dpp filter of its own") {
    // The DPP filter on `q.f5` sits on top of `q` when the rule runs, and cleanup drops it later
    // because of `q.f5 = 51`. Then the DPP filter on `t.f5` reuses the broadcast of `q`, so `t.f5`
    // gets no bloom filter. `t.c5` gets one, since the DPP filter on its key is gone. `q` is on
    // either side of the join.
    Seq(
      "select /*+ broadcast(q) */ * from (select f5, c5, sum(a5) s from bf5part " +
        "where b5 > 0 group by f5, c5) t join bf5filtered q on t.f5 = q.c5 and t.c5 = q.f5 " +
        "where q.a5 > 50 and q.f5 = 51",
      "select /*+ broadcast(q) */ * from bf5filtered q join (select f5, c5, sum(a5) s from " +
        "bf5part where b5 > 0 group by f5, c5) t on q.c5 = t.f5 and q.f5 = t.c5 " +
        "where q.a5 > 50 and q.f5 = 51"
    ).foreach { query =>
      withDppTestConf() {
        assertRewroteWithBloomFilter(query)
        assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryBroadcastExec]))
      }
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
        assertBloomFilterWithoutDpp(query)
        assert(bloomFilterKeys(query).isEmpty)
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

  test("SPARK-60702: keep the dropped dpp filter of the key out of the bloom filter's creation " +
    "side") {
    // `q` is too small to take the bloom filter, so `b` takes it, and `q` is its creation side.
    // The DPP filter on `q.f5` stays out of the bloom filter's subquery. `q` is on either side of
    // the join.
    withTable("bigapp") {
      sql("select a5 + id * 100 as k, b5 from bf5part cross join range(400)")
        .write.saveAsTable("bigapp")
      Seq(
        "select * from bf5filtered q join bigapp b on q.f5 = b.k where q.a5 > 50 and b.b5 = 1",
        "select * from bigapp b join bf5filtered q on b.k = q.f5 where q.a5 > 50 and b.b5 = 1"
      ).foreach { query =>
        withDppTestConf(
            SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "20000") {
          assertRewroteWithBloomFilterForDroppedDpp(query)
          val bloomFilterPlans = sql(query).queryExecution.optimizedPlan.collect {
            case Filter(condition, _) => condition.collect {
              case subquery: ScalarSubquery => subquery.plan
            }
          }.flatten
          assert(bloomFilterPlans.size == 1)
          val creationSide = bloomFilterPlans.head
          assert(creationSide.collectLeaves().exists {
            case LogicalRelationWithTable(_, Some(table)) =>
              table.identifier.table == "bf5filtered"
            case _ => false
          }, creationSide)
          assert(!creationSide.exists {
            case Filter(_: DynamicPruningSubquery, _) => true
            case _ => false
          }, creationSide)
        }
      }
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
      assertBloomFilterWithoutDpp(query)
      if (conf.adaptiveExecutionEnabled) {
        assertRewroteWithBloomFilterForDroppedDpp(query)
      } else {
        assert(bloomFilterKeys(query).isEmpty)
        assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryBroadcastExec]))
      }
    }
  }

  test("SPARK-60702: add bloom filter if the join type rules out the broadcast the dpp filter " +
    "needs") {
    // `bf2` is small enough to broadcast, but a right outer join cannot broadcast its right side,
    // so the DPP filter does not count. The aggregate shuffles below the join, so the join can get
    // the bloom filter.
    val query = "select * from (select f5, sum(a5) s from bf5part group by f5) t " +
      "right join bf2 on t.f5 = bf2.c2 where bf2.a2 = 62"
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000") {
      assertRewroteWithBloomFilterForDroppedDpp(query)
    }
  }

  test("SPARK-60702: add bloom filter if a shuffle hash hint rules out the broadcast the dpp " +
    "filter needs") {
    // `bf2` is small enough to broadcast, but the hint asks for a shuffle hash join. The aggregate
    // shuffles below the join, so the join can get the bloom filter.
    val query = "select /*+ shuffle_hash(bf2) */ * from (select f5, sum(a5) s from bf5part " +
      "group by f5) t join bf2 on t.f5 = bf2.c2 where bf2.a2 = 62"
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000") {
      assertRewroteWithBloomFilterForDroppedDpp(query)
    }
  }

  test("SPARK-60702: do not add bloom filter if the creation side that the subquery reads is not " +
    "deterministic") {
    // The build plan of the DPP filter would be the creation side of the bloom filter. A bloom
    // filter would evaluate it again, apart from the join, and could drop rows (SPARK-60808).
    // Cleanup drops such a DPP filter. Without DPP, SPARK-60808 keeps the bloom filter out too.
    val query = "select * from bf5part join (select * from bf2 where a2 = 36 and " +
      "cast(reflect('java.lang.Math', 'random') as double) < 2.0) d on bf5part.f5 = d.c2"
    withDppTestConf() {
      assertBloomFilterWithoutDpp(query, 0)
      assert(bloomFilterKeys(query).isEmpty)
      assert(dynamicPruningFilters(query).isEmpty)
    }
  }

  test("SPARK-60702: do not add bloom filter whose creation side is a non-deterministic pruned " +
    "side") {
    // Nothing broadcasts `f`, the deterministic build plan of the DPP filter on `d.k`, so that
    // filter is dropped. `d` is too small to take a bloom filter, so `f` would take one, built
    // from `d`, which is not deterministic (SPARK-60808). Without DPP, SPARK-60808 keeps it out.
    withTable("ndd", "ndf") {
      spark.range(40).selectExpr("id as k", "id as w").write.partitionBy("k").saveAsTable("ndd")
      spark.range(40000).selectExpr("id % 40 as k", "id as v").write.saveAsTable("ndf")
      sql("analyze table ndd compute statistics")
      sql("analyze table ndf compute statistics")
      val nddSize = spark.table("ndd").queryExecution.optimizedPlan.stats.sizeInBytes
      val query = "select count(*) from (select k from ndd " +
        "where cast(reflect('java.lang.Math', 'random') as double) < 0.5) d " +
        "join ndf f on d.k = f.k where f.v >= 0"
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key ->
            (nddSize + 1).toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        assertBloomFilterWithoutDpp(query, 0)
        assert(bloomFilterKeys(query).isEmpty)
      }
    }
  }

  test("SPARK-60702: do not add bloom filter if the key of the dropped dpp filter can fail") {
    // On a partition column, the bloom filter would become a partition filter, which evaluates
    // the key on every partition before the data filters (SPARK-60809). An alias hides the key only
    // at the join, since pushdown rewrites the bloom filter through it. And `coalesce` cannot be
    // null, so no inferred `isnotnull` evaluates the key on every partition first.
    // Through a union, every branch counts, and without DPP each gets a bloom filter. A narrowing
    // cast can fail too, also over an up-cast or under the normalization of a floating point key.
    // Each of these keys can fail in ANSI mode only, so the queries are analyzed in it.
    Seq(
      "select * from bf5part join bf2 on bf5part.f5 + 1 = bf2.c2 where bf2.a2 = 36" -> 1,
      "select * from (select coalesce(f5 + 1, 0) as k, a5 from bf5part) p " +
        "join bf2 on p.k = bf2.c2 where bf2.a2 = 36" -> 1,
      "select * from (select f5 k, a5 v from bf5part union all " +
        "select coalesce(f5 + 1, 0) k, a5 v from bf5filtered) u " +
        "join bf2 on u.k = bf2.c2 where bf2.a2 = 36" -> 2,
      "select * from bf5part join bf2 on cast(bf5part.f5 as tinyint) = bf2.c2 " +
        "where bf2.a2 = 36" -> 1,
      "select * from bf5part join bf2 on cast(cast(bf5part.f5 as float) as int) = bf2.c2 " +
        "where bf2.a2 = 36" -> 1,
      "select * from bf5part join (select cast(c2 as double) k, a2 from bf2) d " +
        "on cast(bf5part.f5 as tinyint) = d.k where d.a2 = 36" -> 1
    ).foreach { case (query, numBloomFilters) =>
      withDppTestConf(SQLConf.ANSI_ENABLED.key -> "true") {
        assertBloomFilterWithoutDpp(query, numBloomFilters)
        assert(bloomFilterKeys(query).isEmpty)
        val filters = dynamicPruningFilters(query)
        assert(filters.nonEmpty && filters.forall(_ == Literal.TrueLiteral), filters)
      }
    }
  }

  test("SPARK-60702: do not add bloom filter in an in subquery whose dpp filter reuses a " +
    "broadcast outside it") {
    // The optimizer runs on the subquery on its own, where the broadcast of `x` is not visible.
    // The DPP filter on `p.f5` reuses it once the subquery is joined with the rest of the query.
    val query = "select /*+ broadcast(x) */ * from bf3 join (select c2 from bf2 where a2 = 62) x " +
      "on bf3.c3 = x.c2 where bf3.a3 in (select p.a5 from bf5part p join (select c2 from bf2 " +
      "where a2 = 62) d on p.f5 = d.c2)"
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
      assert(!bloomFilterKeyNames(query).contains("f5"))
      assert(dynamicPruningFilters(query) == Seq(classOf[SubqueryBroadcastExec]))
      // Without the hint, nothing broadcasts `x`, and `p.f5` gets the bloom filter.
      val withoutHint = query.replace("/*+ broadcast(x) */ ", "")
      assert(bloomFilterKeyNames(withoutHint).contains("f5"))
      assert(dynamicPruningFilters(withoutHint) == Seq(Literal.TrueLiteral))
      if (!conf.adaptiveExecutionEnabled) sql(withoutHint).collect()
    }
  }

  test("SPARK-60702: add bloom filter for a key with a dropped dpp filter under a numeric " +
    "up-cast") {
    // Type coercion casts `f5` to `bigint`, which cannot fail.
    val query = "select * from bf5part join (select cast(c2 as bigint) k, a2 from bf2) d " +
      "on bf5part.f5 = d.k where d.a2 = 36"
    withDppTestConf() {
      assertRewroteWithBloomFilterForDroppedDpp(query)
    }
  }

  test("SPARK-60882: add bloom filter for a key and a creation side with a dropped dpp filter " +
    "that can not fail with ANSI mode off") {
    // With ANSI mode off, an addition and a subtraction wrap instead of failing, so the key or the
    // creation side with them gets a bloom filter, and the query returns the same rows as without
    // it. With ANSI mode on, they can fail.
    val inKey = "select * from bf5part join bf2 on bf5part.f5 - 1 = bf2.c2 where bf2.a2 = 57"
    val inCreationSide = "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 + 1 " +
      "where bf2.a2 - 1 = 56"
    Seq(inKey, inCreationSide).foreach { query =>
      withDppTestConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assert(sql(query).collect().nonEmpty, query)
        assertRewroteWithBloomFilterForDroppedDpp(query)
      }
      withDppTestConf(SQLConf.ANSI_ENABLED.key -> "true") {
        assert(bloomFilterKeys(query).isEmpty, query)
      }
    }
  }

  test("SPARK-60702: add bloom filter for a key with a dropped dpp filter of a floating point " +
    "type") {
    // `NormalizeFloatingNumbers` wraps the `double` join keys, which cannot fail, also over a
    // `coalesce`.
    Seq("bf5part.f5", "coalesce(cast(bf5part.f5 as double), 0D)").foreach { key =>
      val query = "select * from bf5part join (select cast(c2 as double) k, a2 from bf2) d " +
        s"on $key = d.k where d.a2 = 36"
      withDppTestConf() {
        assertRewroteWithBloomFilterForDroppedDpp(query)
      }
    }
  }

  test("SPARK-60702: add bloom filter whose creation filter has a coalesce, which cannot fail") {
    val query = "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 " +
      "where coalesce(bf2.a2, 0) = 36"
    withDppTestConf() {
      assertRewroteWithBloomFilterForDroppedDpp(query)
    }
  }

  test("SPARK-60702: check each cast in the creation side against its own child") {
    // Type coercion puts an up-cast into the `coalesce`. The first query casts the `coalesce` down,
    // which can fail in ANSI mode, and the second casts it up. Neither may fail to optimize.
    val narrow = "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 " +
      "where cast(coalesce(bf2.a2, 0L) as int) = 36"
    val wide = "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 " +
      "where coalesce(bf2.a2, 0L) > 35.5D"
    // A deeply nested filter takes a time linear in its size.
    val nested = (1 to 28).foldLeft("b2 + 1") { (acc, k) =>
      s"if(d2 > $k, $acc, cast(b2 as decimal(${10 + k}, 0)))"
    }
    withDppTestConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertBloomFilterWithoutDpp(narrow)
      assert(bloomFilterKeys(narrow).isEmpty)
      assertRewroteWithBloomFilterForDroppedDpp(wide)
      assert(bloomFilterKeys("select * from bf5part join bf2 on bf5part.f5 = bf2.c2 " +
        s"where bf2.a2 = 36 and $nested > 0").isEmpty)
    }
  }

  test("SPARK-60702: do not add bloom filter whose creation side reads a scan with a pushed " +
    "top-n") {
    // The source picks the rows of a pushed top-N, so a second read can pick other rows. So `p.f5`
    // gets no bloom filter. `x.k` gets one, from `bf5part`, which the inferred `f5 > 1` filters.
    withTempView("tn") {
      spark.read.format(classOf[StorageFilterTestSource].getName).option("push", "topN").load()
        .createOrReplaceTempView("tn")
      val query = "select * from bf5part p join (select * from (select * from tn order by v " +
        "limit 100) where k > 1) x on p.f5 = x.k"
      withDppTestConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
          assert(bloomFilterKeyNames(query) == Seq("f5"))
        }
        assert(bloomFilterKeyNames(query) == Seq("k"))
      }
    }
  }

  test("SPARK-60702: do not add bloom filter whose creation side reads a file metadata column") {
    // `_metadata.file_block_start` depends on how the scan splits its files, which the bloom
    // filter's subquery can do differently from the join.
    val query = "select * from bf5part p join bf2 d on p.f5 = d.c2 " +
      "where d.a2 = 36 and d._metadata.file_block_start = 0"
    withDppTestConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      assertBloomFilterWithoutDpp(query)
      assert(bloomFilterKeys(query).isEmpty)
    }
  }

  test("SPARK-60702: do not add bloom filter for a key with a dropped dpp filter under a date to " +
    "timestamp cast") {
    // `Cast.canUpCast` takes a date to a timestamp, which can overflow past the year 294247.
    withTable("fdt", "tsd") {
      sql("create table fdt (v int, d date) using parquet partitioned by (d)")
      sql("insert into fdt select cast(id as int), date'2020-01-01' from range(100)")
      sql("create table tsd (ts timestamp, flag int) using parquet")
      sql("insert into tsd values (timestamp'2020-01-01 00:00:00', 1), " +
        "(timestamp'2021-01-01 00:00:00', 0)")
      val query = "select * from fdt join tsd on fdt.d = tsd.ts where tsd.flag = 1"
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        assertBloomFilterWithoutDpp(query)
        assert(bloomFilterKeys(query).isEmpty)
      }
    }
  }

  test("SPARK-60702: do not add bloom filter for a key with a dropped dpp filter under a " +
    "decimal to bigint cast") {
    // `9999999999999999999` overflows `BIGINT`, so the cast is no up-cast (SPARK-60943), and it
    // fails in ANSI mode. The query casts it only on rows with `v > 0`, which that partition does
    // not have.
    withTable("fd") {
      sql("create table fd (v int, d decimal(19,0)) using parquet partitioned by (d)")
      sql("insert into fd select cast(id as int) + 1, 36 from range(100)")
      sql("insert into fd select -cast(id as int), cast('9999999999999999999' as decimal(19,0)) " +
        "from range(100)")
      val query = "select * from fd join bf2 on cast(fd.d as bigint) = bf2.c2 " +
        "where fd.v > 0 and bf2.a2 = 36"
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
          SQLConf.ANSI_ENABLED.key -> "true") {
        assertBloomFilterWithoutDpp(query)
        assert(bloomFilterKeys(query).isEmpty)
        sql(query).collect()
      }
    }
  }

  test("SPARK-60702: do not add bloom filter whose key can fail in a union branch the lineage " +
    "does not reach") {
    // The lineage of `u.k` stops at the inner union of the second branch, but pushdown moves the
    // bloom filter into both inner branches, where it would cast `p` on every partition. Each
    // row of `p = 'abc'` has `v <= 0`, so the query itself never casts `'abc'`, which fails in ANSI
    // mode only.
    withTable("sp") {
      sql("create table sp (v int, p string) using parquet partitioned by (p)")
      sql("insert into sp select cast(id as int) + 1, '36' from range(100)")
      sql("insert into sp select -cast(id as int), 'abc' from range(100)")
      val query = "select * from (select f5 as k, a5 as v from bf5part union all " +
        "select coalesce(cast(x as int), 0) as k, sum(v) as v from (select p as x, v from sp " +
        "where v > 0 union all select p, v from sp where v > 0) group by x) u " +
        "join bf2 on u.k = bf2.c2 where bf2.a2 = 36"
      withDppTestConf(SQLConf.ANSI_ENABLED.key -> "true") {
        // Without DPP, pushdown puts a bloom filter into each of the three branches.
        assertBloomFilterWithoutDpp(query, 3)
        assert(bloomFilterKeys(query).isEmpty)
        sql(query).collect()
      }
    }
  }

  test("SPARK-60702: add bloom filter whose creation side has a non-deterministic column that " +
    "the bloom filter does not read") {
    // The bloom filter's subquery reads only `c2` of `d`, so `src` does not change its keys. The
    // DPP filter is not deterministic, so cleanup removes it.
    val query = "select * from bf5part p join (select c2, input_file_name() src from bf2 " +
      "where a2 = 36) d on p.f5 = d.c2"
    withDppTestConf() {
      assert(bloomFilterKeyNames(query) == Seq("f5"))
      val expected = withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected)
    }
  }

  test("SPARK-60702: add bloom filter from a cache only if spark can tell that scanning it " +
    "again gives the same rows") {
    // A cache that is not materialized yet can compute a partition twice, once for the join and
    // once for the bloom filter's subquery. So can a cache whose plan Spark cannot show to be
    // repeatable. `bf2c` would give the same rows again, but Spark can tell only once it is
    // materialized.
    def query(cache: String): String =
      s"select * from bf5part p join $cache d on p.f5 = d.c2 where d.a2 = 36"
    withTempView("bf2c", "bf2r") {
      withCache("bf2c", "bf2r") {
        spark.table("bf2").createOrReplaceTempView("bf2c")
        spark.catalog.cacheTable("bf2c")
        withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          assertBloomFilterWithoutDpp(query("bf2c"))
          assert(bloomFilterKeys(query("bf2c")).isEmpty)
          spark.table("bf2c").count()
          assert(bloomFilterKeyNames(query("bf2c")) == Seq("f5"))
          spark.table("bf2").where("cast(reflect('java.lang.Math', 'random') as double) < 2.0")
            .createOrReplaceTempView("bf2r")
          spark.catalog.cacheTable("bf2r")
          spark.table("bf2r").count()
          assertBloomFilterWithoutDpp(query("bf2r"))
          assert(bloomFilterKeys(query("bf2r")).isEmpty)
        }
      }
    }
  }

  test("SPARK-60702: do not add bloom filter for a key with a dropped dpp filter of a collated " +
    "string type") {
    // The bloom filter hashes the bytes of the key, so `'abc'` and `'ABC'` would not match.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      withTable("testcat.cf", "testcat.cd") {
        sql("create table testcat.cf (k string collate utf8_lcase, v int) partitioned by (k)")
        sql("insert into testcat.cf values ('abc', 1)")
        sql("create table testcat.cd (k string collate utf8_lcase, flag int)")
        sql("insert into testcat.cd values ('ABC', 1), ('zzz', 0)")
        val query = "select v from testcat.cf where k in (select k from testcat.cd where flag = 1)"
        withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0") {
          // Nothing wraps the semi join's keys in CollationKey, so they get no bloom filter
          // without DPP either (SPARK-60871).
          assertBloomFilterWithoutDpp(query, numBloomFilters = 0)
          withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
            checkAnswer(sql(query), Row(1))
          }
          assert(bloomFilterKeys(query).isEmpty)
          checkAnswer(sql(query), Row(1))
        }
      }
    }
  }

  test("SPARK-60702: do not add bloom filter whose creation side reads a cte") {
    // The reference to `d` hides that its plan is not deterministic. With exchange reuse off, the
    // bloom filter's subquery would draw a second sample of `d`.
    withTable("cdim", "cfact") {
      spark.range(200).selectExpr("cast(id as int) k", "cast(id % 2 as int) x")
        .write.saveAsTable("cdim")
      spark.range(20000).selectExpr("cast(id % 20 as int) k", "id v")
        .write.partitionBy("k").saveAsTable("cfact")
      sql("analyze table cdim compute statistics")
      sql("analyze table cfact compute statistics")
      val query = "with d as (select k, x from cdim " +
        "where cast(reflect('java.lang.Math', 'random') as double) < 0.5) " +
        "select count(*), count_if(t.k is null) from d left join (select k, sum(v) s from cfact " +
        "group by k) t on d.k = t.k where d.x = 1 union all select count(*), 0 from d"
      withDppTestConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        assertBloomFilterWithoutDpp(query)
        assert(bloomFilterKeys(query).isEmpty)
      }
    }
  }

  test("SPARK-60702: do not add bloom filter whose subquery can fail on partitions that the join " +
    "does not read") {
    // The bloom filter's subquery reads its creation side as it is when this rule runs. A DPP
    // filter on `c.q` that physical planning keeps prunes `rc` only in the join. It reuses the
    // broadcast of `d` in an upper join, and pushdown moves it into `rc` only later. Or it reuses
    // the broadcast of `g` in another branch of a union, which the subquery does not see. So the
    // subquery would read `q = 99` too, and cast `'abc'`, in its key or in its filter.
    withTable("ra", "rc", "rd", "rx") {
      sql("create table ra (v int, x int, b int, a int) using parquet partitioned by (a)")
      sql("insert into ra select cast(id as int), 1, 10, 1 from range(2000)")
      sql("insert into ra select cast(id as int), 1, 20, 2 from range(2000)")
      sql("create table rc (s string, flag int, t int, q int) using parquet partitioned by (q)")
      sql("insert into rc values ('1', 1, 1, 10), ('2', 1, 2, 20), ('abc', 1, 3, 99)")
      sql("create table rd (k int, dflag int) using parquet")
      sql("insert into rd values (10, 1), (20, 1)")
      sql("create table rx (k1 int, k2 int) using parquet")
      sql("insert into rx values (1, 10), (2, 20)")
      // A cast of `'abc'` fails in ANSI mode only.
      val confs = Seq(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
        SQLConf.ANSI_ENABLED.key -> "true")
      val castInKey = "select /*+ broadcast(d) */ f.v from ra f join rc c " +
        "on f.a = cast(c.s as int) join rd d on c.q = d.k " +
        "where f.x = 1 and c.flag = 1 and d.dflag = 1"
      val castInFilter = "select /*+ broadcast(d) */ f.v from ra f join rc c on f.a = c.t " +
        "join rd d on c.q = d.k where f.x = 1 and c.flag = 1 and cast(c.s as int) > 0 " +
        "and d.dflag = 1"
      withDppTestConf(confs: _*) {
        // Without DPP, `f.a` and `c.q` get a bloom filter.
        assertBloomFilterWithoutDpp(castInKey, 2)
        assert(bloomFilterKeys(castInKey).isEmpty)
        assertBloomFilterWithoutDpp(castInFilter, 2)
        // The bloom filter goes to `c.t` instead, from `ra`, where nothing can fail.
        assertRewroteWithBloomFilter(castInFilter)
        assert(bloomFilterKeyNames(castInFilter) == Seq("t"))
        // So it does with each of these filters, which can fail too.
        Seq("(c.t > 5 or cast(c.s as int) > 0)", "not (cast(c.s as int) in (1, 2))",
          "cast(c.s as int) is null", "cast(c.s as int) is not null",
          s"cast(c.s as int) in (${(1 to 11).mkString(", ")})",
          "cast(cast(c.s as int) as boolean)"
        ).foreach { condition =>
          val query = castInFilter.replace("cast(c.s as int) > 0", condition)
          assert(bloomFilterKeyNames(query) == Seq("t"), query)
        }
      }
      val query = "select f.v from ra f join rc c on f.a = cast(c.s as int) and f.b = c.q " +
        "where f.x = 1 and c.flag = 1 union all select /*+ broadcast(g) */ g.v from rx " +
        "join (select * from ra where x = 1) g on rx.k1 = g.a and rx.k2 = g.b"
      withDppTestConf(confs :+ (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
        assertBloomFilterWithoutDpp(query, 2)
        assert(bloomFilterKeys(query).isEmpty)
      }
    }
  }

  test("SPARK-60702: do not add bloom filter whose creation key can fail on rows that another " +
    "bloom filter drops") {
    // The bloom filter on `f.y` drops the row `'abc'` before the join casts it, but only in the
    // join. The bloom filter on `c.q` would read all of `sf` in its subquery.
    withTable("sf", "sc") {
      sql("create table sf (s string, x int, y int) using parquet")
      sql("insert into sf values ('10', 1, 1), ('20', 1, 2), ('abc', 1, 999)")
      sql("create table sc (y int, flag int, q int) using parquet partitioned by (q)")
      sql("insert into sc values (1, 1, 10), (2, 1, 20)")
      val query = "select f.s, c.q from sf f join sc c on f.y = c.y and cast(f.s as int) = c.q " +
        "where f.x = 1 and c.flag = 1"
      // The cast fails in ANSI mode only.
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
          SQLConf.ANSI_ENABLED.key -> "true") {
        assertBloomFilterWithoutDpp(query, 2)
        assert(bloomFilterKeyNames(query) == Seq("y"))
        checkAnswer(sql(query), Seq(Row("10", 10), Row("20", 20)))
      }
    }
  }

  test("SPARK-60702: do not add bloom filter whose creation side keeps a dpp filter that runs as " +
    "its own subquery") {
    // `rc2` is large and `ra2` small, so with `reuseBroadcastOnly` off the DPP filter on `c.q` has
    // a benefit, and the one on `f.a` does not. The first would run as its own subquery in the
    // bloom filter's subquery, though cleanup can drop it from the join. So `f.a` gets no bloom
    // filter. `c.t` gets one, from `ra2`.
    withTable("ra2", "rc2") {
      sql("create table ra2 (v int, x int, b int, a int) using parquet partitioned by (a)")
      sql("insert into ra2 select cast(id as int), 1, 10 + 10 * cast(id % 2 as int), " +
        "1 + cast(id % 2 as int) from range(100)")
      sql("create table rc2 (t int, flag int, pad int, q int) using parquet partitioned by (q)")
      sql("insert into rc2 select 1 + cast(id % 2 as int), 1, cast(id as int), " +
        "10 + 10 * cast(id % 3 as int) from range(20000)")
      sql("analyze table ra2 compute statistics")
      sql("analyze table rc2 compute statistics")
      val query = "select f.v from ra2 f join rc2 c on f.a = c.t and f.b = c.q " +
        "where f.x = 1 and c.flag = 1"
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
        assertRewroteWithBloomFilter(query)
        assert(bloomFilterKeyNames(query) == Seq("t"))
      }
      // With `reuseBroadcastOnly` on, both DPP filters are dropped. The one on `c.q` is `true` in
      // the bloom filter's subquery, so `f.a` gets the bloom filter, and so does `f.b`.
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1") {
        assertRewroteWithBloomFilter(query, 2)
        assert(bloomFilterKeyNames(query).toSet == Set("a", "b"))
      }
    }
  }

  test("SPARK-60702: add bloom filter if the broadcast join builds the side the dpp filter " +
    "prunes") {
    val query = "select /*+ broadcast(t) */ * from " +
      "(select f5, sum(a5) from bf5part group by f5) t join bf2 on t.f5 = bf2.c2 " +
      "where bf2.a2 = 62"
    withDppTestConf() {
      assertRewroteWithBloomFilterForDroppedDpp(query)
    }
  }

  test("SPARK-60702: do not add bloom filter if the dpp filter can project its values from " +
    "a broadcast") {
    def query(broadcast: String): String = "select * from bf5part join " +
      s"(select /*+ broadcast($broadcast) */ bf3.f3 from bf2 join bf3 on bf2.c2 = bf3.c3 " +
      "where bf3.a3 = 62) t on bf5part.f5 = t.f3"
    val projection = SQLConf.DYNAMIC_PARTITION_PRUNING_BROADCAST_PROJECTION_ENABLED.key -> "true"
    withDppTestConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_BROADCAST_PROJECTION_ENABLED.key -> "false") {
      assertRewroteWithBloomFilter(query("bf3"))
    }
    // Nothing can broadcast `bf3`, so there is no broadcast to project from.
    withDppTestConf(projection, SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assertRewroteWithBloomFilterForDroppedDpp(query("bf2"))
    }
    withDppTestConf(projection, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      assert(bloomFilterKeys(query("bf3")).isEmpty)
      assert(dynamicPruningFilters(query("bf3")) ==
        Seq(classOf[ProjectedBroadcastValueSubqueryExec]))
    }
    // AQE plans the projected filter as `true`, see SPARK-60703.
    withDppTestConf(projection, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      assert(bloomFilterKeys(query("bf3")).isEmpty)
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

  test("SPARK-60808: do not add bloom filter whose creation side is not deterministic") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val queries = Seq(
        "select * from bf2 where a2 = 62 and rand() < 0.5",
        "select if(rand() < 0.5, c2, null) c2 from bf2 where a2 = 62",
        // The bloom filter does not read `r`.
        "select c2, rand() r from bf2 where a2 = 62"
      ).map(creationSide => s"select * from bf1 join ($creationSide) t on bf1.c1 = t.c2")
      assert(queries.map(q => getNumBloomFilters(sql(q).queryExecution.optimizedPlan)) ==
        Seq(0, 0, 1))
    }
  }

  test("SPARK-60808: a non-deterministic creation side does not lose join rows") {
    withTable("dim", "fact") {
      sql("CREATE TABLE dim USING parquet AS SELECT cast(id AS INT) k FROM range(200)")
      sql("CREATE TABLE fact USING parquet AS SELECT cast(id % 200 AS INT) k FROM range(20000)")
      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        // Every `dim.k` is in `fact`. `rand()` would pick the same rows in each scan of the same
        // files, so the filter calls `java.lang.Math.random` instead.
        checkAnswer(sql(
          """SELECT count_if(f.k IS NULL)
            |FROM (SELECT k FROM dim
            |  WHERE cast(reflect('java.lang.Math', 'random') AS DOUBLE) < 0.5) d
            |LEFT JOIN fact f ON d.k = f.k""".stripMargin), Row(0L))
        // A deterministic creation side gets a bloom filter with these tables and settings.
        assert(getNumBloomFilters(sql("SELECT * FROM (SELECT k FROM dim WHERE k < 100) d " +
          "LEFT JOIN fact f ON d.k = f.k").queryExecution.optimizedPlan) == 1)
      }
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
