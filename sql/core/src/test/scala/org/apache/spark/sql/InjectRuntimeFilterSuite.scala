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
import java.sql.DriverManager
import java.util.Properties

import org.apache.logging.log4j.Level

import org.apache.spark.sql.catalyst.analysis.HintErrorLogger
import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, DynamicPruning, DynamicPruningExpression, DynamicPruningSubquery, Expression, Literal, NamedExpression, ScalarSubquery, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, SHUFFLE_MERGE}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.connector.StorageFilterTestSource
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, LogicalRDD, ProjectedBroadcastValueSubqueryExec, ReusedSubqueryExec, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEPropagateEmptyRelation}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{LogicalRelation, LogicalRelationWithTable}
import org.apache.spark.sql.execution.planmerging.MergeSubplans
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{count, lit, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

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
    // Counts the Bloom filter aggregates at the root of each Bloom filter subquery, where every
    // aggregate expression must be one. Aggregates below the root belong to a hinted creation
    // side (e.g. a `SELECT DISTINCT` one) and are not visited; a subquery whose root aggregate
    // has no Bloom filter aggregate is the query's own.
    def isBloomFilterAgg(e: NamedExpression): Boolean = e.exists {
      case AggregateExpression(_: BloomFilterAggregate, _, _, _, _) => true
      case _ => false
    }
    def countBloomFilterAggs(plan: LogicalPlan): Int = plan match {
      case Aggregate(_, aggregateExpressions, _, _)
          if aggregateExpressions.exists(isBloomFilterAgg) =>
        aggregateExpressions.map {
          case Alias(AggregateExpression(bfAgg : BloomFilterAggregate, _, _, _, _),
          _) =>
            assert(bfAgg.estimatedNumItemsExpression.isInstanceOf[Literal])
            assert(bfAgg.numBitsExpression.isInstanceOf[Literal])
            1
        }.sum
      case _: Aggregate => 0
      case other => other.children.map(countBloomFilterAggs).sum
    }
    val numBloomFilterAggs = plan.collect {
      case Filter(condition, _) => condition.collect {
        case subquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery =>
          countBloomFilterAggs(subquery.plan)
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

  /**
   * [[assertRewroteWithBloomFilter]] that also requires the query to return rows, so that comparing
   * the answers with and without the filter would catch a filter that drops matching rows.
   */
  def assertHintedRewrite(query: String, runtimeFilterNum: Int = 1): Unit = {
    assertRewroteWithBloomFilter(query, runtimeFilterNum)
    assertReturnsRows(query)
  }

  def assertReturnsRows(query: String): Unit = {
    assert(sql(query).collect().nonEmpty, s"expected the query to return rows: $query")
  }

  /**
   * Returns the join keys the query's runtime Bloom filters are applied on. Apart from `bf5part`
   * and `bf5filtered`, which share a schema, the tables use their own attribute names (`c1` in
   * `bf1`, `c2` in `bf2`, ...), so this identifies which side a filter was applied to.
   */
  def bloomFilterApplicationSideKeys(query: String): Set[String] = {
    sql(query).queryExecution.optimizedPlan.collect {
      case Filter(condition, _) => condition.collect {
        case BloomFilterMightContain(_, XxHash64(Seq(key), _)) => key.references.map(_.name)
      }.flatten
    }.flatten.toSet
  }

  /**
   * Optimizes `query`, and plans it when `planned` is set, and returns the warnings the hint error
   * handler logged while doing so.
   */
  def hintWarnings(query: String, planned: Boolean = false): Seq[String] = {
    val logAppender = new LogAppender("runtime filter hint")
    withLogAppender(logAppender, level = Some(Level.WARN)) {
      val queryExecution = sql(query).queryExecution
      if (planned) queryExecution.executedPlan else queryExecution.optimizedPlan
    }
    val hintLogger = HintErrorLogger.getClass.getName.stripSuffix("$")
    logAppender.loggingEvents.filter(_.getLoggerName == hintLogger)
      .map(_.getMessage.getFormattedMessage).toSeq
  }

  def assertHintNotApplied(query: String, reason: String): Unit = {
    val warnings = hintWarnings(query)
    assert(warnings.exists(_.contains(s"is not supported in the query: $reason")),
      s"expected a warning mentioning '$reason', got: $warnings")
  }

  /** Asserts `query` gets no runtime Bloom filter, without executing it. */
  def assertNoBloomFilters(query: String): Unit = {
    assert(getNumBloomFilters(sql(query).queryExecution.optimizedPlan) == 0)
  }

  def hasDynamicPruning(query: String): Boolean = {
    sql(query).queryExecution.optimizedPlan.exists {
      case Filter(condition, _) => condition.exists(_.isInstanceOf[DynamicPruningSubquery])
      case _ => false
    }
  }

  // Whether `query` has a DPP filter that honors a RUNTIME_FILTER hint.
  def hasHintedDynamicPruning(query: String): Boolean = {
    sql(query).queryExecution.optimizedPlan.exists(_.expressions.exists(_.exists {
      case dpp: DynamicPruningSubquery =>
        dpp.getTagValue(DynamicPruning.HONORS_RUNTIME_FILTER_HINT).isDefined
      case _ => false
    }))
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

  test("Runtime bloom filter join: add bloom filter for a key with a dropped dpp filter of two " +
    "collated string columns") {
    // A join of two collated columns is on their collation keys, whose bytes the bloom filter can
    // hash, while the IN subquery of the test above compares the strings themselves.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      withTable("testcat.cf", "testcat.cd") {
        sql("create table testcat.cf (k string collate utf8_lcase, v int) partitioned by (k)")
        sql("insert into testcat.cf values ('abc', 1)")
        sql("create table testcat.cd (k string collate utf8_lcase, flag int)")
        sql("insert into testcat.cd values ('ABC', 1), ('zzz', 0)")
        val query = "select v from testcat.cf f join testcat.cd d on f.k = d.k where d.flag = 1"
        withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0") {
          assert(hasDynamicPruning(query))
          assert(bloomFilterKeys(query).size == 1)
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

  test("Runtime bloom filter join: add bloom filter if the dpp filter cannot reach the scan") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
      spark.udf.register("bf_nondeterministic_true", udf((_: Int) => true).asNondeterministic())
      // The non-deterministic filter keeps the DPP filter from being pushed to the scan, so it is
      // dropped before this rule runs, and the key is left to a Bloom filter. With
      // `reuseBroadcastOnly` off, the DPP filter runs as its own subquery, so it would keep the
      // Bloom filter off the key if this rule ran before the cleanup.
      val query = "select * from (select * from bf5part where bf_nondeterministic_true(f5)) f " +
        "join bf2 on f.f5 = bf2.c2 where bf2.a2 = 62"
      assertRewroteWithBloomFilter(query)
      assert(!hasDynamicPruning(query))
    }
  }

  test("Runtime bloom filter join: a dpp filter pushed down from a join above does not block the " +
    "bloom filter of a join below") {
    // The DPP filter on `f5` is pushed into `bf5part`, the creation side of the join below. The
    // `LIKE` in its build plan would stop the search for that creation side, but the filter is
    // `onlyInBroadcast`, so it is `true` in the bloom filter's subquery and left out of it.
    val query = "select * from bf1 join bf5part on bf1.c1 = bf5part.c5 " +
      "join bf2 on bf5part.f5 = bf2.c2 " +
      "where bf5part.a5 = 5 and bf2.a2 = 62 and cast(bf2.b2 as string) like '%1%2%'"
    withDppTestConf() {
      assert(hasDynamicPruning(query))
      assertRewroteWithBloomFilter(query, 2)
      assert(bloomFilterKeyNames(query).toSet == Set("c1", "c2"))
    }
  }

  test("Runtime bloom filter join: do not add bloom filter if the dpp filter reuses a broadcast " +
    "that dpp prunes on two keys") {
    // The build plan of the DPP filter on `t.f5` is the broadcast `b`, whose DPP filters on `p1`
    // and `p2` it keeps stacked, as `PartitionPruning` built them. Pushdown combines them in `b`.
    withTable("bf_dpp2_part", "bf_dpp2_dim") {
      sql("create table bf_dpp2_part (k int, v int, p1 int, p2 int) using parquet " +
        "partitioned by (p1, p2)")
      sql("insert into bf_dpp2_part select cast(id as int), cast(id % 10 as int), " +
        "cast(id % 2 as int), cast(id % 3 as int) from range(100)")
      sql("create table bf_dpp2_dim (x int, y int, z int) using parquet")
      sql("insert into bf_dpp2_dim values (0, 0, 1), (1, 2, 0)")
      val query = "select /*+ broadcast(b) */ * from " +
        "(select f5, sum(a5) from bf5part group by f5) t join " +
        "(select dp.k from bf_dpp2_part dp join bf_dpp2_dim d on dp.p1 = d.x and dp.p2 = d.y " +
        "where d.z = 1 and dp.v = 3) b on t.f5 = b.k"
      withDppTestConf() {
        assert(hasDynamicPruning(query))
        assert(bloomFilterKeys(query).isEmpty)
        assertBloomFilterWithoutDpp(query)
      }
    }
  }

  test("Runtime bloom filter join: do not add bloom filter if a union branch other than the " +
    "first keeps the dpp filter") {
    // Pushdown takes the DPP filter on `t.f5` into both branches of the union. Cleanup drops it
    // from the first, whose static filter on the partition column makes it unnecessary. The one
    // in the second reuses the broadcast of `bf2`.
    val query = "select /*+ broadcast(bf2) */ * from (select f5, sum(c5) from " +
      "(select f5, c5 from bf5part where f5 = 1 union all select f5, c5 from bf5filtered) " +
      "group by f5) t join bf2 on t.f5 = bf2.c2 where bf2.a2 = 62"
    withDppTestConf() {
      assert(hasDynamicPruning(query))
      assert(bloomFilterKeys(query).isEmpty)
      // Without DPP, the bloom filter is pushed into both branches.
      assertBloomFilterWithoutDpp(query, 2)
    }
  }

  test("Runtime bloom filter join: add bloom filter if a dpp filter that runs as its own " +
    "subquery is pushed down into the creation side") {
    withTable("bf_za", "bf_zb", "bf_zc") {
      spark.range(2000).selectExpr("id AS v", "CAST(id % 100 AS INT) AS k")
        .write.saveAsTable("bf_za")
      spark.range(2000).selectExpr("CAST(id % 100 AS INT) AS k", "CAST(id % 10 AS INT) AS x",
        "CAST(id % 20 AS INT) AS part").write.partitionBy("part").saveAsTable("bf_zb")
      spark.range(20).selectExpr("CAST(id AS INT) AS p", "concat('n', id) AS name")
        .write.saveAsTable("bf_zc")
      // The join with `zc` pushes its DPP filter down into `zb`, the creation side of the bloom
      // filter on `za.k`. Its build plan has a `LIKE`, which is not a simple expression.
      val query = "SELECT za.v, zc.name FROM bf_za za JOIN bf_zb zb ON za.k = zb.k " +
        "JOIN bf_zc zc ON zb.part = zc.p WHERE zb.x = 1 AND zc.name LIKE '_1'"
      withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
        assert(hasDynamicPruning(query))
        assertRewroteWithBloomFilter(query)
        assertReturnsRows(query)
      }
    }
  }

  test("Runtime bloom filter join: do not add bloom filter if dpp filter exists " +
    "on the same column through a renaming projection") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
      val query = "select * from (select f5 as k from bf5part) f join bf2 on f.k = bf2.c2 " +
        "where bf2.a2 = 62"
      assertDidNotRewriteWithBloomFilter(query)
      assert(hasDynamicPruning(query))
    }
  }

  test("Runtime bloom filter join: add bloom filter on a key fixed by a literal") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // A static partition filter fixes the partition key, so its DPP filter is dropped as
      // redundant. A Bloom filter on that key keeps every row or none, which still prunes the
      // whole side when the other side lacks the literal.
      val query = "select * from bf5part f join bf2 on f.f5 = bf2.f2 where f.f5 = 20"
      assertRewroteWithBloomFilter(query)
      assert(!hasDynamicPruning(query))
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

  test("RUNTIME_FILTER hint waives the creation side's selective predicate requirement") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val query = "select * from bf4 join bf2 on bf4.c4 = bf2.c2"
      // Without a selective predicate on either side there is no evidence a filter pays off.
      assertDidNotRewriteWithBloomFilter(query)
      // The hint supplies that evidence, and picks which side to build from.
      assertHintedRewrite(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2")
      assert(bloomFilterApplicationSideKeys(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2") == Set("c4"))
      assert(bloomFilterApplicationSideKeys(
        "select /*+ RUNTIME_FILTER(bf4) */ * from bf4 join bf2 on bf4.c4 = bf2.c2") == Set("c2"))
    }
  }

  test("RUNTIME_FILTER hint waives the application side's scan size threshold") {
    // A threshold above the application side's scan size, so only the hint can trigger a rewrite.
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1GB",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assertDidNotRewriteWithBloomFilter(
        "select * from bf4 join bf2 on bf4.c4 = bf2.c2 where bf2.a2 = 62")
      assertHintedRewrite(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2")
    }
  }

  test("RUNTIME_FILTER hint waives the creation side's size threshold") {
    // A threshold below any creation side's size, so only the hint can trigger a rewrite. The
    // threshold estimates whether the filter is worth building; the filter's own size is bounded
    // by the max number of bits regardless.
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assertDidNotRewriteWithBloomFilter(
        "select * from bf4 join bf2 on bf4.c4 = bf2.c2 where bf2.a2 = 62")
      assertHintedRewrite(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2")
    }
  }

  test("RUNTIME_FILTER hint waives the shuffle requirement") {
    // `bf2` is below the broadcast threshold, so this is a broadcast join with no shuffle below
    // the application side, where a filter is not expected to pay off.
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000") {
      assertDidNotRewriteWithBloomFilter(
        "select * from bf4 join bf2 on bf4.c4 = bf2.c2 where bf2.a2 = 62")
      assertHintedRewrite(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2")
    }
  }

  test("RUNTIME_FILTER hint waives the application side's single-leaf lineage requirement") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The application side's key is an aggregate result, which cannot be traced to a scan. The
      // heuristics use that lineage to predict that the filter reaches a scan; the filter above
      // the aggregate is valid regardless, and prunes the aggregate's output before the join.
      val aggregatedApplicationSide =
        """
          |FROM   (SELECT max(c4) AS c4 FROM bf4 GROUP BY b4) t
          |       JOIN bf2
          |       ON t.c4 = bf2.c2
        """.stripMargin
      assertDidNotRewriteWithBloomFilter(s"SELECT * $aggregatedApplicationSide WHERE bf2.a2 = 62")
      assertHintedRewrite(
        s"SELECT /*+ RUNTIME_FILTER(bf2) */ * $aggregatedApplicationSide")
    }
  }

  test("RUNTIME_FILTER hint accepts a creation side the heuristics reject") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Left to itself the rule looks for a selective predicate over a scan, a search an
      // `Aggregate` stops, so this is not rewritten despite the selective predicate below it. The
      // hint designates the creation side instead of searching for one, so the shape stops
      // mattering.
      val distinctSource =
        """
          |FROM   bf4
          |       JOIN (SELECT DISTINCT c2 FROM bf2 WHERE a2 = 5) t
          |       ON bf4.c4 = t.c2
        """.stripMargin
      assertDidNotRewriteWithBloomFilter(s"SELECT * $distinctSource")
      assertHintedRewrite(s"SELECT /*+ RUNTIME_FILTER(t) */ * $distinctSource")
    }
  }

  test("RUNTIME_FILTER hint builds a filter from a computed join key") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The filter is built from the hinted side's output, so a join key the creation side computes
      // rather than reads from a scan -- here an aggregate result -- works like any other.
      assertHintedRewrite(
        """
          |SELECT /*+ RUNTIME_FILTER(t) */ *
          |FROM   bf4
          |       JOIN (SELECT max(c2) AS c2 FROM bf2 GROUP BY b2) t
          |       ON bf4.c4 = t.c2
        """.stripMargin)
    }
  }

  test("RUNTIME_FILTER hint adds no Bloom filter on a key a DPP filter prunes") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      // `bf5part` is partitioned by `f5`, and the selective `bf2.a2 = 10` makes DPP prune it on
      // that key. The hint is honored by that DPP filter, so no Bloom filter is added on that key,
      // matching the unhinted behavior asserted above.
      val oneKey = "select /*+ RUNTIME_FILTER(bf2) */ * from bf5part join bf2 " +
        "on bf5part.f5 = bf2.f2 where bf2.a2 = 10"
      assertDidNotRewriteWithBloomFilter(oneKey)
      assertReturnsRows(oneKey)
      assert(hasDynamicPruning(oneKey))
      assert(hintWarnings(oneKey).isEmpty)
      // The other join key still gets a Bloom filter, as without the hint ("add bloom filter if
      // dpp filter exists on a different column" above).
      val twoKeys = "select /*+ RUNTIME_FILTER(bf2) */ * from bf5part join bf2 " +
        "on bf5part.c5 = bf2.c2 and bf5part.f5 = bf2.f2 where bf2.a2 = 10"
      assertHintedRewrite(twoKeys)
      assert(bloomFilterApplicationSideKeys(twoKeys) == Set("c5"))
      assert(hasDynamicPruning(twoKeys))
      assert(hintWarnings(twoKeys).isEmpty)
    }
  }

  test("RUNTIME_FILTER hint is honored by a DPP filter only when that filter reaches the scan") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      spark.udf.register("bf_nondeterministic_true", udf((_: Int) => true).asNondeterministic())
      // The hint gets a DPP filter on `bf5part`'s partition key, but a barrier on the pruned side
      // keeps it from being pushed to the scan, so it is dropped later: a non-deterministic
      // filter, or a window whose partitioning does not cover the key. The Bloom filter needs no
      // pushdown, so the hint is honored with one instead, and exactly one of the two remains.
      Seq(
        ("SELECT f5 FROM bf5part WHERE bf_nondeterministic_true(f5)", 1),
        ("SELECT f5 FROM (SELECT f5, row_number() OVER (PARTITION BY c5 ORDER BY f5) rn " +
          "FROM bf5part) WHERE rn = 1", 1),
        // The DPP filter is pushed into both union branches, where the limit blocks it and the
        // `Range` cannot use it. The Bloom filter is pushed into both branches too. The key of the
        // second branch is a `TRY_CAST`, as a key that can fail gets no hinted filter.
        ("SELECT f5 FROM (SELECT f5 FROM bf5part ORDER BY f5 LIMIT 2) " +
          "UNION ALL SELECT TRY_CAST(id AS INT) AS f5 FROM range(3)", 2)
      ).foreach { case (prunedSide, numBloomFilters) =>
        val query = "SELECT /*+ RUNTIME_FILTER(bf2) */ f.f5, bf2.c2 " +
          s"FROM ($prunedSide) f JOIN bf2 ON f.f5 = bf2.c2"
        assertHintedRewrite(query, numBloomFilters)
        assert(!hasDynamicPruning(query))
      }
      // A DPP filter that does reach the scan honors the hint on its own, even when the pruned
      // side is not deterministic elsewhere (here on the other side of a join the DPP filter is
      // pushed past), so no Bloom filter is added on top of it.
      val reaching = "SELECT /*+ RUNTIME_FILTER(bf2) */ f.f5, f.nd, bf2.c2 FROM " +
        "(SELECT p.f5, b.nd FROM bf5part p JOIN " +
        "(SELECT c3, bf_nondeterministic_true(a3) AS nd FROM bf3) b ON p.c5 = b.c3) f " +
        "JOIN bf2 ON f.f5 = bf2.c2"
      assertDidNotRewriteWithBloomFilter(reaching)
      assertReturnsRows(reaching)
      assert(hasDynamicPruning(reaching))
      assert(hintWarnings(reaching).isEmpty)
    }
  }

  test("RUNTIME_FILTER hint honored by DPP stays honored after a DPP filter is pushed into it") {
    import IntegratedUDFTestUtils._
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("rdd_side") {
        val rdd = spark.sparkContext.parallelize(Seq(Row(51), Row(20), Row(0)))
        spark.createDataFrame(rdd, new StructType().add("k", IntegerType))
          .createOrReplaceTempView("rdd_side")
        // The hinted `d` honors the hint with a DPP filter on `f`. The upper join then prunes `d`
        // with a DPP filter built from an RDD, which is pushed into `d`. That filter could only
        // read a broadcast, so it never evaluates the RDD again, and physical planning drops it
        // here. So `d` is still the source of the hinted DPP filter, which is built again from it.
        val query = "SELECT /*+ RUNTIME_FILTER(d) */ f.c5, d.f5 FROM bf5part f " +
          "JOIN bf5filtered d ON f.f5 = d.f5 " +
          "JOIN (SELECT k FROM rdd_side WHERE k > 1) r ON d.f5 = r.k"
        val plan = sql(query).queryExecution.optimizedPlan
        val buildsFromRdd = plan.exists {
          case Filter(condition, _) => condition.exists {
            case d: DynamicPruningSubquery => d.buildQuery.exists(_.isInstanceOf[LogicalRDD])
            case _ => false
          }
          case _ => false
        }
        assert(buildsFromRdd, plan.treeString)
        assert(hasHintedDynamicPruning(query))
        assertDidNotRewriteWithBloomFilter(query)
        assertReturnsRows(query)
        assert(hintWarnings(query).isEmpty)
        // The same when the build plan of that filter calls a Python UDF, which the hinted side
        // does not evaluate for it either.
        if (shouldTestPythonUDFs) {
          registerTestUDF(TestPythonUDF(name = "bf_py_int", returnType = Some(IntegerType)), spark)
          val udf = query.replace("SELECT k FROM rdd_side WHERE k > 1",
            "SELECT bf_py_int(a2) AS k FROM bf2 WHERE a2 > 1")
          assert(hasHintedDynamicPruning(udf))
          assertReturnsRows(udf)
          assert(hintWarnings(udf).isEmpty)
        }
      }
    }
  }

  test("RUNTIME_FILTER hint is ignored for a creation side that is not repeatable") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The filter subquery re-executes the creation side, so one that can produce different rows
      // or keys on re-evaluation could yield keys the join itself never sees and prune rows that
      // do match. Catalyst flags most of these as deterministic.
      val reason = "the hinted side may produce different rows or join keys when evaluated again"
      spark.udf.register("bf_nondeterministic_array",
        udf((i: Int) => Seq(i)).asNondeterministic())
      spark.udf.register("bf_nondeterministic_int", udf((i: Int) => i).asNondeterministic())
      def query(source: String): String =
        s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN ($source) t ON bf4.c4 = t.c2"
      Seq(
        // A non-deterministic expression.
        "SELECT c2 FROM bf2 WHERE rand() < 0.5",
        // A limit or an offset keeps whichever rows arrive first, even over a total order: the
        // planner keeps the sort order only in a take-ordered shape, which a bare offset or a
        // column-pruned copy of a top-n does not have.
        "SELECT c2 FROM bf2 WHERE a2 = 62 LIMIT 5",
        "SELECT c2 FROM bf2 WHERE a2 = 62 ORDER BY c2 LIMIT 5",
        "SELECT c2 FROM bf2 GROUP BY c2 ORDER BY c2 LIMIT 5",
        "SELECT c2, max(a2) AS m FROM bf2 GROUP BY c2 ORDER BY c2 LIMIT 5",
        "SELECT c2 FROM bf2 GROUP BY c2 ORDER BY c2 OFFSET 2",
        "SELECT c2 FROM bf2 GROUP BY c2 ORDER BY c2 LIMIT 10 OFFSET 2",
        // A sample draws a different sample per evaluation without a seed, and per partition
        // with one, while each evaluation chooses its own partitioning.
        "SELECT c2 FROM bf2 TABLESAMPLE (50 PERCENT)",
        "SELECT c2 FROM bf2 TABLESAMPLE (50 PERCENT) REPEATABLE (42)",
        // A key computed by an order-dependent aggregate function.
        "SELECT first(c2) AS c2 FROM bf2 GROUP BY b2",
        "SELECT any_value(c2) AS c2 FROM bf2 GROUP BY b2",
        // A key computed by a window function.
        "SELECT row_number() OVER (ORDER BY c2) AS c2 FROM bf2",
        // An inner generate drops the rows for which an unstable generator yields nothing.
        "SELECT c2 FROM bf2 LATERAL VIEW explode(bf_nondeterministic_array(a2)) x AS v",
        // An inner join null-safely comparing an order-dependent value changes the row set.
        "SELECT x.b2 AS c2 FROM (SELECT b2, first(c2) AS f FROM bf2 GROUP BY b2) x " +
          "JOIN bf3 ON x.f <=> bf3.c3",
        // A key computed by a subquery whose own plan is not repeatable.
        "SELECT (SELECT max(c2) FROM bf2 TABLESAMPLE (50 PERCENT)) AS c2 FROM bf2",
        // A scalar subquery whose row count depends on a floating point sum may return more than
        // one row.
        "SELECT (SELECT 1 FROM (SELECT sum(CAST(c2 AS DOUBLE)) AS x FROM bf2) s " +
          "LATERAL VIEW OUTER explode(sequence(1, least(CAST(x AS INT), 1))) t AS v) AS c2 " +
          "FROM bf2",
        // An outer generate keeps every row but repeats it as many times as the unstable
        // generator yields values, so an aggregate over the rows is unstable.
        "SELECT count(*) AS c2 FROM bf2 " +
          "LATERAL VIEW OUTER explode(bf_nondeterministic_array(a2)) x AS v",
        // An average of integers accumulates in floating point, which is not associative.
        "SELECT avg(c2) AS c2 FROM bf2 GROUP BY b2",
        // An overflow of a partial sum turns the sum into null for good, and whether a partial
        // sum overflows depends on how the rows are split.
        "SELECT try_sum(c2) AS c2 FROM bf2 GROUP BY b2",
        // Among values that are equal but not identical, a group, a distinct row, a minimum or a
        // maximum keeps whichever arrives first, which a key telling them apart exposes.
        "SELECT CAST(s COLLATE UTF8_BINARY AS INT) AS c2 FROM " +
          "(SELECT CAST(c2 AS STRING) COLLATE UTF8_LCASE AS s FROM bf2) GROUP BY s",
        "SELECT CAST(s COLLATE UTF8_BINARY AS INT) AS c2 FROM " +
          "(SELECT DISTINCT CAST(c2 AS STRING) COLLATE UTF8_LCASE AS s FROM bf2)",
        "SELECT max(CAST(c2 AS DOUBLE)) AS c2 FROM bf2 GROUP BY b2",
        // Grouping on an unstable value varies how many groups share the stable grouping values,
        // so a count of them is unstable, and so is whether distinct rows collapse.
        "SELECT count(*) AS c2 FROM (SELECT b2 FROM " +
          "(SELECT b2, bf_nondeterministic_int(a2) AS r FROM bf2) GROUP BY b2, r) GROUP BY b2",
        "SELECT count(*) AS c2 FROM " +
          "(SELECT DISTINCT b2, bf_nondeterministic_int(a2) AS r FROM bf2) GROUP BY b2"
      ).foreach { source =>
        assertNoBloomFilters(query(source))
        assertHintNotApplied(query(source), reason)
      }
      // So does a decimal sum outside ANSI mode, while ANSI mode fails the query on an overflow.
      val decimalSum =
        "SELECT CAST(sum(CAST(c2 AS DECIMAL(38, 0))) AS INT) AS c2 FROM bf2 GROUP BY b2"
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assertNoBloomFilters(query(decimalSum))
        assertHintNotApplied(query(decimalSum), reason)
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        assertRewroteWithBloomFilter(query(decimalSum))
      }
    }
  }

  test("RUNTIME_FILTER hint accepts a creation side whose unstable values do not reach the key") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Only the key values and the row set have to be repeatable. An order-dependent value that
      // is merely carried to the output, next to a stable key, does not disqualify the side, and
      // neither does a filter on a window aggregate whose frame is the whole partition.
      def query(source: String, key: String): String =
        s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN ($source) t ON bf4.c4 = t.$key"
      spark.udf.register("bf_nondeterministic_array",
        udf((i: Int) => Seq(i)).asNondeterministic())
      Seq(
        ("SELECT b2, first(c2) AS c2 FROM bf2 GROUP BY b2", "b2"),
        ("SELECT c2, v FROM bf2 LATERAL VIEW OUTER explode(bf_nondeterministic_array(a2)) x AS v",
          "c2"),
        ("SELECT (SELECT max(c2) FROM bf2) AS c2, b2 FROM bf2", "c2"),
        ("SELECT sum(c2) AS c2 FROM bf2 GROUP BY b2", "c2"),
        ("SELECT c2, sum(a2) OVER (PARTITION BY b2 ORDER BY c2 " +
          "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM bf2", "c2"),
        ("SELECT c2, row_number() OVER (ORDER BY a2) AS rn FROM bf2", "c2"),
        ("SELECT c2, sum(a2) OVER (PARTITION BY b2) AS s FROM bf2", "c2"),
        ("SELECT c2 FROM (SELECT c2, sum(a2) OVER (PARTITION BY b2) AS s FROM bf2) WHERE s > 0",
          "c2"),
        // Grouping on a stable value yields each group once, whatever the input multiplicity.
        ("SELECT c2, count(*) AS n FROM bf2 " +
          "LATERAL VIEW OUTER explode(bf_nondeterministic_array(a2)) x AS v GROUP BY c2", "c2"),
        ("SELECT DISTINCT c2 FROM bf2 " +
          "LATERAL VIEW OUTER explode(bf_nondeterministic_array(a2)) x AS v", "c2")
      ).foreach { case (source, key) =>
        assertHintedRewrite(query(source, key))
      }
      // Once an unstable value decides which rows survive, the row set is not repeatable.
      val reason = "the hinted side may produce different rows or join keys when evaluated again"
      Seq(
        ("SELECT c2 FROM (SELECT c2, row_number() OVER (PARTITION BY b2 ORDER BY a2) AS rn " +
          "FROM bf2) WHERE rn = 1", "c2"),
        ("SELECT b2 FROM bf2 GROUP BY b2 HAVING first(c2) > 0", "b2")
      ).foreach { case (source, key) =>
        assertNoBloomFilters(query(source, key))
        assertHintNotApplied(query(source, key), reason)
      }
    }
  }

  test("RUNTIME_FILTER hint keeps an outer generate whose rows the hinted side counts") {
    withTable("bf_gen_dim", "bf_gen_fact") {
      sql("CREATE TABLE bf_gen_dim (k INT, arr ARRAY<INT>) USING parquet")
      sql("INSERT INTO bf_gen_dim VALUES (1, array(1, 2)), (2, array(1))")
      sql("CREATE TABLE bf_gen_fact (k INT, v INT) USING parquet")
      sql("INSERT INTO bf_gen_fact VALUES (1, 10), (2, 20)")
      // Nothing reads the generated column, but `count(*)` counts the rows the generate repeats.
      val query = "SELECT /*+ RUNTIME_FILTER(d) */ f.k, f.v FROM bf_gen_fact f JOIN " +
        "(SELECT k FROM (SELECT k, count(*) c FROM bf_gen_dim " +
        "LATERAL VIEW OUTER explode(arr) e AS x GROUP BY k) WHERE c > 1) d ON f.k = d.k"
      Seq("true", "false").foreach { adaptive =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive) {
          assertRewroteWithBloomFilter(query)
          checkAnswer(sql(query), Row(1, 10))
        }
      }
    }
  }

  test("RUNTIME_FILTER hint keeps an outer generate whose output a union passes up") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The union outputs the generated column of its second branch by position, without
      // referencing it, and the grouping reads it. So the filter's subquery keeps the generate,
      // whose generator is not deterministic.
      val query = "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN (SELECT k FROM " +
        "(SELECT c2 AS k, b2 AS g FROM bf2 UNION ALL SELECT c3, v FROM bf3 " +
        "LATERAL VIEW OUTER explode(shuffle(array(a3, b3))) x AS v) GROUP BY k, g) t " +
        "ON bf4.c4 = t.k"
      assertNoBloomFilters(query)
      assertHintNotApplied(query,
        "the runtime filter's subquery over the hinted side is not deterministic")
    }
  }

  test("RUNTIME_FILTER hint keeps correctness requirements") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // A left outer join may not have its left side pruned: dropping a left row would drop the
      // null-extended output row it is entitled to.
      val leftOuter =
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 left outer join bf2 on bf4.c4 = bf2.c2"
      assertDidNotRewriteWithBloomFilter(leftOuter)
      assertHintNotApplied(leftOuter, "the left side of a left outer join cannot be pruned")
      // The mirror case is allowed: the right side of a left outer join is prunable.
      assertHintedRewrite(
        "select /*+ RUNTIME_FILTER(bf4) */ * from bf4 left outer join bf2 on bf4.c4 = bf2.c2")
      // Non-simple expressions stay excluded, as the filter subquery would recompute them.
      spark.udf.register("bf_square", (s: Long) => s * s)
      val udfKey =
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf_square(bf2.c2)"
      assertDidNotRewriteWithBloomFilter(udfKey)
      assertHintNotApplied(udfKey, "the join key is not a simple expression")
    }
  }

  test("RUNTIME_FILTER hint accepts a creation side that carries a runtime filter of its own") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The inner join gets a Bloom filter on `bf4.c4` from the selective `bf2` on its own. The
      // hinted outer join uses that join as its source; the filter it carries is a stable value,
      // so a second filter is built from the source for `bf3`.
      val query = "select /*+ RUNTIME_FILTER(t) */ * from bf3 join " +
        "(select bf4.c4 from bf4 join bf2 on bf4.c4 = bf2.c2 where bf2.a2 = 5) t " +
        "on bf3.c3 = t.c4"
      assertHintedRewrite(query, 2)
      assert(bloomFilterApplicationSideKeys(query) == Set("c4", "c3"))
    }
  }

  test("RUNTIME_FILTER hint reports a creation side it cannot analyze") {
    withTempView("bf2_typed") {
      spark.table("bf2").filter((_: Row) => true).createOrReplaceTempView("bf2_typed")
      val query = "select /*+ RUNTIME_FILTER(bf2_typed) */ * from bf4 join bf2_typed " +
        "on bf4.c4 = bf2_typed.c2"
      assertNoBloomFilters(query)
      assertHintNotApplied(query,
        "the hinted side contains TypedFilter, which cannot be checked for repeatability")
    }
  }

  test("RUNTIME_FILTER hint on a side that cannot be a source leaves the join as without it") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Left to itself the rule builds a filter from the selective `bf2` and applies it to `t`.
      val join = "FROM (SELECT c4 FROM bf4 ORDER BY c4 LIMIT 5) t JOIN bf2 ON t.c4 = bf2.c2 " +
        "WHERE bf2.a2 = 62"
      assertRewroteWithBloomFilter(s"SELECT * $join")
      // `t` is not a repeatable source, so the hint is reported and the heuristics build the
      // filter they build without it.
      val hinted = s"SELECT /*+ RUNTIME_FILTER(t) */ * $join"
      assertRewroteWithBloomFilter(hinted)
      assert(bloomFilterApplicationSideKeys(hinted) == Set("c4"))
      assertHintNotApplied(hinted,
        "the hinted side may produce different rows or join keys when evaluated again")
      // `t` can be a source, but a left outer join cannot prune its left side. The hint is
      // reported, and the filter the heuristics would build from `bf2` into `t` is not built.
      val outerJoin = "FROM bf2 LEFT JOIN (SELECT c4 FROM bf4) t ON bf2.c2 = t.c4 " +
        "WHERE bf2.a2 = 62"
      assertRewroteWithBloomFilter(s"SELECT * $outerJoin")
      val outerHinted = s"SELECT /*+ RUNTIME_FILTER(t) */ * $outerJoin"
      assertDidNotRewriteWithBloomFilter(outerHinted)
      assertHintNotApplied(outerHinted, "the left side of a left outer join cannot be pruned")
    }
  }

  test("RUNTIME_FILTER hint inside a subquery is applied once the subquery becomes a join") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
      // An uncorrelated subquery is optimized on its own, filter included, before it is hoisted
      // into the main plan, where the rule meets the filter it injected itself.
      val hoisted = "select * from bf4 where bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf3) */ bf2.c2 from bf2 join bf3 on bf2.c2 = bf3.c3)"
      assertHintedRewrite(hoisted)
      assert(!hintWarnings(hoisted).exists(_.contains("is not supported")))
      // A correlated subquery is decorrelated into a join first; the rule then visits its joins.
      val decorrelated = "select * from bf4 where exists (select /*+ RUNTIME_FILTER(bf3) */ 1 " +
        "from bf2 join bf3 on bf2.c2 = bf3.c3 where bf2.a2 = bf4.a4)"
      assertHintedRewrite(decorrelated)
      assert(!hintWarnings(decorrelated).exists(_.contains("is not supported")))
      // A hint that fails is reported once, not again when the hoisted join is visited.
      val failing = "select * from bf4 where bf4.c4 in (select /*+ RUNTIME_FILTER(t) */ bf2.c2 " +
        "from bf2 join (select c3 from bf3 limit 5) t on bf2.c2 = t.c3)"
      assertNoBloomFilters(failing)
      assert(hintWarnings(failing).count(_.contains("is not supported")) == 1)
      // The same when the hinted join's subtree renames a column: a failure is reported once, and
      // a filter built on a renamed key, which pushdown moves below the rename, is built once.
      val renamedFailing = "select * from bf4 where bf4.c4 in (select /*+ RUNTIME_FILTER(t) */ " +
        "bf2.c2 from bf2 join (select c3 as k from bf3 limit 5) t on bf2.c2 = t.k)"
      assert(hintWarnings(renamedFailing).count(_.contains("is not supported")) == 1)
      assertHintedRewrite("select * from bf4 where bf4.c4 in (select /*+ RUNTIME_FILTER(bf3) */ " +
        "t.k from (select c2 as k from bf2) t join bf3 on t.k = bf3.c3)")
      // And when a DPP filter from the outer query is pushed through the hoisted join, which
      // rebuilds it: the join carries no hint by then.
      val pushedThroughFailing = "select * from bf4 where bf4.a4 = 10 and bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(t) */ p.f5 from bf5part p " +
        "join (select c3 from bf3 limit 5) t on p.c5 = t.c3)"
      assert(hintWarnings(pushedThroughFailing).count(_.contains("is not supported")) == 1)
      // A hint that is not applied for another reason stays on the join, where the decision is
      // kept for the pass over the outer query.
      val pushedThroughBlocked = "select * from bf4 where bf4.a4 = 10 and bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf3) */ p.f5 from bf5part p left join bf3 on p.c5 = bf3.c3)"
      assert(hintWarnings(pushedThroughBlocked).count(_.contains("is not supported")) == 1)
      assertHintNotApplied(pushedThroughBlocked,
        "the left side of a left outer join cannot be pruned")
      val pushedThroughRenamed = "select * from bf4 where bf4.a4 = 10 and bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf3) */ t.f5 from (select f5, c5 as k from bf5part) t " +
        "join bf3 on t.k = bf3.c3)"
      assert(hasDynamicPruning(pushedThroughRenamed))
      assert(bloomFilterApplicationSideKeys(pushedThroughRenamed) == Set("c5"))
      assert(getNumBloomFilters(sql(pushedThroughRenamed).queryExecution.optimizedPlan) == 1)
      val ambiguous = "select * from bf4 where bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf2, bf3) */ bf2.c2 from bf2 join bf3 on bf2.c2 = bf3.c3)"
      assert(hintWarnings(ambiguous).count(
        _.contains("the runtime filter source is ambiguous")) == 1)
      // A DPP filter from the selective `bf4` is pushed through the hinted join between the two
      // passes, which rebuilds the join; the filter built from `bf3` still counts as applied.
      val rebuilt = "select * from bf4 where bf4.a4 = 10 and bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf3) */ p.f5 from bf5part p join bf3 on p.c5 = bf3.c3)"
      assert(hasDynamicPruning(rebuilt))
      assert(bloomFilterApplicationSideKeys(rebuilt).contains("c5"))
      assert(!hintWarnings(rebuilt).exists(_.contains("is not supported")))
    }
  }

  test("RUNTIME_FILTER hint inside a subquery counts a merged filter as applied") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      // The two filters built from `bf3` inside the subquery are merged into one plan before the
      // rebuilt join is visited again in the main plan.
      val query = "select * from bf4 where bf4.a4 = 10 and bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf3) */ p.f5 from bf5part p join bf3 " +
        "on p.c5 = bf3.c3 and p.b5 = bf3.b3)"
      assert(hasDynamicPruning(query))
      assert(bloomFilterApplicationSideKeys(query) == Set("c5", "b5"))
      assert(!hintWarnings(query).exists(_.contains("is not supported")))
    }
  }

  test("Bloom filter pushdown leaves a DPP filter on the scan") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The Bloom filter on `p` is added after the DPP filter from `bf4` is pushed down and
      // cleaned up, and its own pushdown combines the two above the scan.
      val query = "select * from bf4 where bf4.a4 = 10 and bf4.c4 in " +
        "(select /*+ RUNTIME_FILTER(bf3) */ p.f5 from bf5part p join bf3 on p.c5 = bf3.c3)"
      val plan = sql(query).queryExecution.optimizedPlan
      val scanFilters = plan.collect {
        case Filter(condition, _: LogicalRelation) => condition
      }
      assert(scanFilters.exists { condition =>
        condition.exists(_.isInstanceOf[DynamicPruningSubquery]) &&
          condition.exists(_.isInstanceOf[BloomFilterMightContain])
      }, plan.treeString)
      assert(!plan.exists {
        case Filter(_, _: LogicalRelation) => false
        case Filter(condition, _) => condition.exists(_.isInstanceOf[DynamicPruningSubquery])
        case _ => false
      }, plan.treeString)
      assert(!plan.exists {
        case Filter(condition, _) => condition.exists(_ == Literal.TrueLiteral)
        case _ => false
      }, plan.treeString)
    }
  }

  test("RUNTIME_FILTER hint that is not applied leaves DPP to its own estimates") {
    // The selective `bf3` side broadcasts, so DPP reuses the broadcast without the hint. With the
    // hint on that side, which is not repeatable, DPP still does, and the hint is reported as not
    // applied rather than credited to that DPP filter.
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      def query(hint: String): String = s"select $hint * from bf5part p join " +
        "(select c3 from bf3 tablesample (50 percent) where a3 = 10) t on p.f5 = t.c3"
      assert(hasDynamicPruning(query("")))
      val hinted = query("/*+ RUNTIME_FILTER(t) */")
      assert(hasDynamicPruning(hinted))
      assertNoBloomFilters(hinted)
      assertHintNotApplied(hinted,
        "the hinted side may produce different rows or join keys when evaluated again")
    }
  }

  test("RUNTIME_FILTER hint rejects a join key that may take different values") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The filter evaluates both join keys separately from the join, so a subquery inside a key
      // is analyzed like one inside the source.
      val sample = "coalesce((select max(c2) from bf2 tablesample (50 percent)), 0)"
      val creationKey =
        s"select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2 + $sample"
      assertNoBloomFilters(creationKey)
      assertHintNotApplied(creationKey,
        "the hinted side may produce different rows or join keys when evaluated again")
      val applicationKey =
        s"select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 + $sample = bf2.c2"
      assertNoBloomFilters(applicationKey)
      assertHintNotApplied(applicationKey,
        "the join key may take different values when evaluated again")
    }
  }

  test("RUNTIME_FILTER hint rejects a DPP pruning key that may take different values") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Without subquery reuse, the scan filter and join evaluate the sampled key independently.
      val sample = "coalesce((select max(c2) from bf2 tablesample (50 percent)), 0)"
      Seq("bf5part join bf2", "bf2 join bf5part").foreach { join =>
        val query = s"select /*+ RUNTIME_FILTER(bf2) */ * from $join " +
          s"on bf5part.f5 + $sample = bf2.c2"
        assert(!hasDynamicPruning(query))
        assertNoBloomFilters(query)
        assertHintNotApplied(query, "the join key may take different values when evaluated again")
        val seeded = query.replace("(50 percent)", "(50 percent) repeatable (42)")
        assert(!hasDynamicPruning(seeded))
        assertNoBloomFilters(seeded)
      }
    }
  }

  test("RUNTIME_FILTER hint requires a source Spark can read again with the same rows") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("rdd_side", "checkpointed_side", "observed_side") {
        val schema = new StructType().add("c2", IntegerType)
        val rdd = spark.sparkContext.parallelize(Seq(Row(45), Row(0)))
        // An RDD's lineage is opaque, so only a checkpointed one is known to be repeatable.
        spark.createDataFrame(rdd, schema).createOrReplaceTempView("rdd_side")
        spark.createDataFrame(rdd, schema).localCheckpoint(eager = true)
          .createOrReplaceTempView("checkpointed_side")
        // The filter would evaluate an observation a second time under the same name.
        spark.table("bf2").observe("bf2_rows", count(lit(1)))
          .createOrReplaceTempView("observed_side")
        def query(view: String): String =
          s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN $view t ON bf4.c4 = t.c2"
        assertNoBloomFilters(query("rdd_side"))
        assertHintNotApplied(query("rdd_side"),
          "the hinted side reads LogicalRDD, which may return different rows when evaluated again")
        assertHintedRewrite(query("checkpointed_side"))
        assertNoBloomFilters(query("observed_side"))
        assertHintNotApplied(query("observed_side"),
          "the hinted side contains CollectMetrics, whose observation the runtime filter would " +
            "evaluate twice")
      }
      // A file metadata column such as `_metadata.file_block_start` depends on how the scan splits
      // its files, which the filter's own scan can do differently. The threshold keeps the
      // heuristics, which then decide the join, from building a filter.
      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1GB") {
        val metadata = "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN " +
          "(SELECT c2 FROM bf2 WHERE a2 = 62 AND _metadata.file_block_start = 0) t " +
          "ON bf4.c4 = t.c2"
        assertNoBloomFilters(metadata)
        assertHintNotApplied(metadata,
          "the hinted side may produce different rows or join keys when evaluated again")
      }
    }
  }

  test("RUNTIME_FILTER hint analyzes unions, grouping sets, offsets and repartitioning") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      spark.udf.register("bf_nondeterministic_array",
        udf((i: Int) => Seq(i)).asNondeterministic())
      def query(source: String): String =
        s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN ($source) t ON bf4.c4 = t.c2"
      Seq(
        "SELECT c2 FROM bf2 UNION ALL SELECT c3 FROM bf3",
        "SELECT c2, b2, count(*) AS n FROM bf2 GROUP BY GROUPING SETS ((c2), (c2, b2))",
        "SELECT /*+ REPARTITION(3) */ c2 FROM bf2",
        "SELECT /*+ REPARTITION(3, c2) */ c2 FROM bf2",
        "SELECT /*+ REPARTITION_BY_RANGE(3, c2) */ c2 FROM bf2",
        "SELECT /*+ REBALANCE(c2) */ c2 FROM bf2"
      ).foreach(source => assertHintedRewrite(query(source)))
      val outerExplode = "LATERAL VIEW OUTER explode(bf_nondeterministic_array(a2)) x AS v"
      val reason = "the hinted side may produce different rows or join keys when evaluated again"
      Seq(
        // An unstable row set in one branch.
        "SELECT c2 FROM bf2 UNION ALL SELECT c3 FROM bf3 WHERE rand() < 0.5",
        // An offset skips whichever rows arrive first.
        "SELECT c2 FROM bf2 OFFSET 2",
        // Unstable multiplicity carried through a union, a join and grouping sets.
        s"SELECT count(*) AS c2 FROM (SELECT b2 FROM bf2 $outerExplode " +
          "UNION ALL SELECT b3 FROM bf3) GROUP BY b2",
        s"SELECT count(*) AS c2 FROM (SELECT b2 FROM bf2 $outerExplode) t " +
          "JOIN bf3 ON t.b2 = bf3.b3 GROUP BY t.b2",
        s"SELECT count(*) AS c2 FROM (SELECT a2, b2 FROM bf2 $outerExplode) " +
          "GROUP BY GROUPING SETS ((b2), (b2, a2))"
      ).foreach { source =>
        assertNoBloomFilters(query(source))
        assertHintNotApplied(query(source), reason)
      }
    }
  }

  test("RUNTIME_FILTER hint accepts a cache only once it is materialized") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("materialized_side", "lazy_side") {
        sql("CACHE TABLE materialized_side AS SELECT c2 FROM bf2 WHERE a2 > 0")
        sql("CACHE LAZY TABLE lazy_side AS SELECT c2 FROM bf2 WHERE a2 > 1")
        def query(view: String): String =
          s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN $view t ON bf4.c4 = t.c2"
        assertHintedRewrite(query("materialized_side"))
        assertNoBloomFilters(query("lazy_side"))
        assertHintNotApplied(query("lazy_side"),
          "the hinted side reads InMemoryRelation, which is not materialized yet")
      }
    }
  }

  test("RUNTIME_FILTER hint rejects a stream as the creation side") {
    import testImplicits._
    val input = MemoryStream[Int]
    val stream = input.toDF().select($"value".as("k")).hint("runtime_filter")
    val joined = spark.table("bf4").join(stream, $"c4" === $"k")
    val logAppender = new LogAppender("runtime filter hint")
    withLogAppender(logAppender, level = Some(Level.WARN)) {
      val query = joined.writeStream.format("memory").queryName("runtime_filter_stream").start()
      try {
        input.addData(45, 0)
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }
    assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
      "is not supported in the query: the hinted side is a stream")))
  }

  test("RUNTIME_FILTER hint rejects a creation side with a Python UDTF") {
    import IntegratedUDFTestUtils._
    assume(shouldTestPythonUDFs)
    // The filter subquery is planned without the extraction the main plan gets, so it could not
    // evaluate the UDTF, even a deterministic one.
    val udtf = TestPythonUDTF(name = "bf_udtf")
    spark.udtf.registerPython(udtf.name, udtf.udtf.copy(udfDeterministic = true))
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val query = "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN " +
        "(SELECT u.x FROM (SELECT a2, b2 FROM bf2 WHERE a2 IS NOT NULL AND b2 IS NOT NULL) b, " +
        s"LATERAL ${udtf.name}(b.a2, b.b2) u) t ON bf4.c4 = t.x"
      assertDidNotRewriteWithBloomFilter(query)
      assertReturnsRows(query)
      assertHintNotApplied(query,
        "the hinted side contains a Python UDF, Python UDTF or external UDF")
    }
  }

  test("RUNTIME_FILTER hint follows the join type's prunable side") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val rightOuterFromLeft =
        "select /*+ RUNTIME_FILTER(bf4) */ * from bf4 right outer join bf2 on bf4.c4 = bf2.c2"
      assertNoBloomFilters(rightOuterFromLeft)
      assertHintNotApplied(rightOuterFromLeft,
        "the right side of a right outer join cannot be pruned")
      assertHintedRewrite(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 right outer join bf2 on bf4.c4 = bf2.c2")
      val fullOuter =
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 full outer join bf2 on bf4.c4 = bf2.c2"
      assertNoBloomFilters(fullOuter)
      assertHintNotApplied(fullOuter, "the left side of a full outer join cannot be pruned")
      assertHintNotApplied("select /*+ RUNTIME_FILTER(bf2) */ * from bf4 cross join bf2",
        "no equi-join keys")
    }
  }

  test("RUNTIME_FILTER hint builds one filter per join key") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val twoKeys = "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 " +
        "on bf4.c4 = bf2.c2 and bf4.b4 = bf2.b2"
      assertHintedRewrite(twoKeys, 2)
      assert(bloomFilterApplicationSideKeys(twoKeys) == Set("c4", "b4"))
      // Two key pairs on the same key of `bf4` build one filter on it, not one per pair.
      val sameKey = "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 " +
        "on bf4.c4 = bf2.c2 and bf4.c4 = bf2.b2"
      assert(getNumBloomFilters(sql(sameKey).queryExecution.optimizedPlan) == 1)
      assert(!hintWarnings(sameKey).exists(_.contains("is not supported")))
      // A key the filter cannot use does not block the others, and the hint counts as applied.
      spark.udf.register("bf_identity", (s: Int) => s)
      val oneBlocked = "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 " +
        "on bf4.c4 = bf2.c2 and bf4.b4 = bf_identity(bf2.b2)"
      assertHintedRewrite(oneBlocked)
      assert(!hintWarnings(oneBlocked).exists(_.contains("is not supported")))
    }
  }

  test("Runtime bloom filter join: a DPP filter is matched by its pruning key too") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
      // Both join keys are built from `bf2.c2`. DPP prunes the partition key `f5`; the Bloom
      // filter still lands on `c5`, which nothing prunes.
      val query = "select * from bf5part join bf2 on bf5part.f5 = bf2.c2 and bf5part.c5 = bf2.c2 " +
        "where bf2.a2 = 62"
      assertRewroteWithBloomFilter(query)
      assert(bloomFilterApplicationSideKeys(query) == Set("c5"))
      assert(hasDynamicPruning(query))
    }
  }

  test("RUNTIME_FILTER hint accepts a source that carries a DPP filter built from unstable rows") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000") {
      // The inner join prunes `bf5part` with a DPP filter built from `d`, whose `first(c2)` is
      // order-dependent. Only `d`'s build key `b2` reaches that filter, so the hinted side is a
      // repeatable source of `f5`. The Bloom filter's subquery computes `first(c2)` as well: it
      // keeps the columns of the hinted side, to build the broadcast of `d` that the DPP filter
      // reuses.
      val query =
        """
          |SELECT /*+ RUNTIME_FILTER(f) */ *
          |FROM   (SELECT p.f5, d.n
          |        FROM   bf5part p
          |               JOIN (SELECT b2, first(c2) AS n FROM bf2 WHERE a2 < 60 GROUP BY b2) d
          |               ON p.f5 = d.b2) f
          |       JOIN bf3
          |       ON f.f5 = bf3.f3
        """.stripMargin
      assert(hasDynamicPruning(query))
      assert(bloomFilterApplicationSideKeys(query) == Set("f3"))
      assert(hintWarnings(query).isEmpty)
      assertReturnsRows(query)
    }
  }

  test("RUNTIME_FILTER hint never filters the hinted side") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      // Left to itself the rule builds a filter from the selective `bf2` and applies it to `bf4`.
      // Hinting `bf4` as the source designates the opposite direction, and the hinted side is not
      // filtered even though it would have been without the hint.
      val query = "select /*+ RUNTIME_FILTER(bf4) */ * from bf4 join bf2 on bf4.c4 = bf2.c2 " +
        "where bf2.a2 = 5"
      assertHintedRewrite(query)
      assert(bloomFilterApplicationSideKeys(query) == Set("c2"))
    }
  }

  test("RUNTIME_FILTER hint composes with a join strategy hint") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val query =
        "select /*+ MERGE(bf4, bf2), RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2"
      assertHintedRewrite(query)
      val plan = sql(query).queryExecution.optimizedPlan
      val hints = plan.collect { case Join(_, _, _, _, hint) => hint }
      assert(hints.size == 1)
      // Both hints survive on the same side: the strategy hint is not overridden by the runtime
      // filter hint, nor the other way around.
      assert(hints.head.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE)))
      assert(hints.head.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE)))
      assert(hints.head.rightHint.exists(_.runtimeFilterSource))
      assert(!hints.head.leftHint.exists(_.runtimeFilterSource))
      // The plan names the hint as it is written.
      assert(plan.toString.contains("rightHint=(strategy=merge, runtime_filter)"), plan.toString)
      // A warning about one hint does not name the other: overriding the strategy leaves the
      // runtime filter hint in effect, and rejecting the runtime filter hint leaves the strategy.
      val warnings = hintWarnings("select /*+ BROADCAST(bf2), MERGE(bf2), RUNTIME_FILTER(bf2) */ " +
        "* from bf4 left outer join bf2 on bf4.c4 = bf2.c2")
      assert(warnings.exists(_.contains("Hint (strategy=merge) is overridden")))
      assert(warnings.exists(_.contains("Hint (runtime_filter) is not supported")))
      assert(!warnings.exists(_.contains("(strategy=merge, runtime_filter)")))
      // The same when join selection rejects the strategy: an invalid build side, or a join
      // without equi-join keys, whichever side carries the strategy.
      Seq(
        ("select /*+ BROADCAST(bf4), RUNTIME_FILTER(bf4) */ * from bf4 " +
          "left outer join bf2 on bf4.c4 = bf2.c2",
          "Hint (strategy=broadcast) is not supported in the query: build left for left outer"),
        ("select /*+ MERGE(bf2), RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 > bf2.c2",
          "Hint (strategy=merge) is not supported in the query: no equi-join keys"),
        ("select /*+ RUNTIME_FILTER(bf4), MERGE(bf2) */ * from bf4 join bf2 on bf4.c4 > bf2.c2",
          "Hint (strategy=merge) is not supported in the query: no equi-join keys")
      ).foreach { case (query, strategyWarning) =>
        val planned = hintWarnings(query, planned = true)
        assert(planned.exists(_.startsWith(strategyWarning)), planned)
        assert(!planned.exists(_.contains(", runtime_filter)")), planned)
      }
    }
  }

  test("RUNTIME_FILTER hint on both join sides is ambiguous and ignored") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      // Each side would have to be the other's creation side, so the hint is dropped and the
      // heuristics decide: the selective `bf2` still yields a filter on `bf4`, as without a hint.
      val query = "select /*+ RUNTIME_FILTER(bf4, bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2 " +
        "where bf2.a2 = 5"
      assertHintedRewrite(query)
      assert(bloomFilterApplicationSideKeys(query) == Set("c4"))
      assertHintNotApplied(query, "the runtime filter source is ambiguous")
      // The same on a join without equi-join keys.
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        assertHintNotApplied(
          "select /*+ RUNTIME_FILTER(bf4, bf2) */ * from bf4 join bf2 on bf4.c4 > bf2.c2",
          "the runtime filter source is ambiguous")
      }
    }
    // The same in an IN subquery: once it is joined with the outer query, the join gets the bloom
    // filter on `f5` in place of its dropped DPP filter, as without a hint.
    withDppTestConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val subquery = "select * from bf4 where bf4.c4 in (select %s p.f5 from bf5part p " +
        "join bf2 on p.f5 = bf2.c2 where bf2.a2 = 62)"
      assert(bloomFilterKeyNames(subquery.format("")).sorted == Seq("c4", "f5"))
      assert(bloomFilterKeyNames(subquery.format("/*+ RUNTIME_FILTER(p, bf2) */")).sorted ==
        Seq("c4", "f5"))
    }
  }

  test("RUNTIME_FILTER hint warns with the reason when it cannot be applied") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      // A filter on the key from `bf2` was already injected at the inner join; the hinted side
      // still adds its own.
      val existing = "select /*+ RUNTIME_FILTER(bf3) */ * from bf4 join bf2 on bf4.c4 = bf2.c2 " +
        "join bf3 on bf3.c3 = bf4.c4 where bf2.a2 = 5"
      assertHintedRewrite(existing, 2)
      assert(!hintWarnings(existing).exists(_.contains("is not supported")))
      // The filter count limit applies to hinted filters too.
      withSQLConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD.key -> "1") {
        assertHintedRewrite(existing)
        assertHintNotApplied(existing,
          s"${SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD.key} (1) is reached")
      }
      // With Bloom filters disabled the hint is still reported rather than silently dropped,
      // unless a DPP filter honors it.
      withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        assertHintNotApplied(
          "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 = bf2.c2",
          s"${SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key} is false")
        val dppHonored = "select /*+ RUNTIME_FILTER(bf2) */ * from bf5part join bf2 " +
          "on bf5part.f5 = bf2.f2 where bf2.a2 = 10"
        assert(hasDynamicPruning(dppHonored))
        assert(getNumBloomFilters(sql(dppHonored).queryExecution.optimizedPlan) == 0)
        assert(hintWarnings(dppHonored).isEmpty)
      }
    }
  }

  test("RUNTIME_FILTER hint on a subquery planned as an existence join is reported") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // Under OR, the subquery becomes an existence join whose hinted right side would filter
      // the left side, which an existence join cannot prune.
      Seq(
        "SELECT * FROM bf4 WHERE bf4.c4 IN " +
          "(SELECT /*+ RUNTIME_FILTER(bf2) */ c2 FROM bf2 WHERE a2 = 62) OR bf4.a4 > 90",
        "SELECT * FROM bf4 WHERE EXISTS " +
          "(SELECT /*+ RUNTIME_FILTER(bf2) */ * FROM bf2 WHERE bf2.c2 = bf4.c4 AND a2 = 62) " +
          "OR bf4.a4 > 90"
      ).foreach { query =>
        assertNoBloomFilters(query)
        assertHintNotApplied(query, "the left side of an existence join cannot be pruned")
        checkAnswer(sql(query),
          sql(query.replace("/*+ RUNTIME_FILTER(bf2) */", "")).collect().toSeq)
      }
    }
  }

  test("RUNTIME_FILTER hint on a relation that is not part of a join is reported") {
    val logAppender = new LogAppender("runtime filter hint")
    withLogAppender(logAppender, level = Some(Level.WARN)) {
      sql("select /*+ RUNTIME_FILTER(bf4) */ * from bf4").queryExecution.optimizedPlan
    }
    assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
      "A join hint (runtime_filter) is specified but it is not part of a join relation")))
  }

  test("RUNTIME_FILTER hint on a join without equi-join keys is reported") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertHintNotApplied(
        "select /*+ RUNTIME_FILTER(bf2) */ * from bf4 join bf2 on bf4.c4 > bf2.c2",
        "no equi-join keys")
    }
  }

  test("RUNTIME_FILTER hint inside a subquery applies to the join it is rewritten into") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // An IN subquery becomes a left semi join, whose left side can be pruned.
      assertHintedRewrite(
        "select * from bf4 where bf4.c4 in (select /*+ RUNTIME_FILTER(bf2) */ c2 from bf2)")
      // A NOT EXISTS subquery becomes a left anti join, whose left side cannot.
      val notExists = "select * from bf4 where not exists " +
        "(select /*+ RUNTIME_FILTER(bf2) */ 1 from bf2 where bf2.c2 = bf4.c4)"
      assertDidNotRewriteWithBloomFilter(notExists)
      assertHintNotApplied(notExists, "the left side of a left anti join cannot be pruned")
    }
  }

  test("RUNTIME_FILTER hint without parameters applies to the subtree below it") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val query = "select * from bf4 join (select /*+ RUNTIME_FILTER */ c2 from bf2) t " +
        "on bf4.c4 = t.c2"
      assertHintedRewrite(query)
      assert(bloomFilterApplicationSideKeys(query) == Set("c4"))
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

  test("Runtime filters do not prune a stream with a stateful operator") {
    import testImplicits._
    // A filter is built again in each micro-batch, so the rows it drops in one would be missing
    // from the aggregate state once the static side gains their key.
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      Seq(true, false).foreach { hinted =>
        withTable("testcat.ns.stream_dim") {
          sql("CREATE TABLE testcat.ns.stream_dim (k INT, v STRING)")
          sql("INSERT INTO testcat.ns.stream_dim VALUES (1, 'a')")
          val input = MemoryStream[Int]
          val counts = input.toDF().groupBy($"value".as("k")).count()
          val dim = spark.table("testcat.ns.stream_dim").where($"k" < 100)
          val name = s"stateful_stream_$hinted"
          val query = counts.join(if (hinted) dim.hint("runtime_filter") else dim, "k")
            .writeStream.format("memory").queryName(name).outputMode("update").start()
          try {
            input.addData(1, 2)
            query.processAllAvailable()
            sql("INSERT INTO testcat.ns.stream_dim VALUES (2, 'b')")
            input.addData(2)
            query.processAllAvailable()
            checkAnswer(spark.table(name), Seq(Row(1, 1L, "a"), Row(2, 2L, "b")))
          } finally {
            query.stop()
          }
        }
      }
    }
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(100).selectExpr("id v", "CAST(id % 10 AS INT) p").write.partitionBy("p")
        .mode("overwrite").parquet(path)
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
        Seq(true, false).foreach { hinted =>
          val counts = spark.readStream.schema("v BIGINT, p INT").parquet(path)
            .groupBy($"p").count()
          val dim = spark.table("bf2").where($"a2" === 62)
          val query = counts.join(if (hinted) dim.hint("runtime_filter") else dim,
              $"p" === $"c2")
            .writeStream.format("memory").queryName(s"stateful_file_stream_$hinted")
            .outputMode("update").start()
          try {
            query.processAllAvailable()
            val plan = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
              .optimizedPlan
            assert(!plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[DynamicPruning]))),
              plan.treeString)
          } finally {
            query.stop()
          }
        }
      }
    }
  }

  test("RUNTIME_FILTER hint refuses a collated key that is not a plain column") {
    withTable("bf_collated_fact", "bf_collated_dim") {
      sql("CREATE TABLE bf_collated_fact (k STRING COLLATE UTF8_LCASE) USING parquet")
      sql("CREATE TABLE bf_collated_dim (k STRING COLLATE UTF8_LCASE, c INT) USING parquet")
      sql("INSERT INTO bf_collated_fact SELECT 'abcdef' FROM range(100)")
      sql("INSERT INTO bf_collated_dim VALUES ('ABC', 1), ('ABCDEF', 2)")
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        // The Bloom filter hashes the bytes of a key, while the join compares it under its
        // collation (SPARK-58486).
        val expression = "SELECT /*+ RUNTIME_FILTER(d) */ * FROM bf_collated_fact f " +
          "JOIN bf_collated_dim d ON substr(f.k, 1, 3) = d.k WHERE d.c = 1"
        assertNoBloomFilters(expression)
        assert(sql(expression).count() == 100)
        assertHintNotApplied(expression,
          "the join key has a collation the Bloom filter hash does not follow")
        // A plain column is joined on its collation key, which compares by its bytes.
        assertHintedRewrite("SELECT /*+ RUNTIME_FILTER(d) */ * FROM bf_collated_fact f " +
          "JOIN bf_collated_dim d ON f.k = d.k WHERE d.c = 2")
        // A group outputs whichever of its values arrives first, all with the same collation key.
        Seq("SELECT DISTINCT k FROM bf_collated_dim",
          "SELECT k, count(*) AS n FROM bf_collated_dim GROUP BY k").foreach { side =>
          assertHintedRewrite("SELECT /*+ RUNTIME_FILTER(d) */ * FROM bf_collated_fact f " +
            s"JOIN ($side) d ON f.k = d.k")
        }
      }
    }
  }

  test("RUNTIME_FILTER hint takes a V1 JDBC source as it takes a DSv2 one") {
    // A batch read is taken to return the same rows every time. A source that picks its own rows
    // on each read, such as a JDBC query with a `LIMIT`, is for the user not to hint.
    Utils.classForName("org.h2.Driver")
    val url = "jdbc:h2:mem:bf_hint_v1;DB_CLOSE_DELAY=-1"
    val conn = DriverManager.getConnection(url)
    try {
      conn.prepareStatement("CREATE TABLE dim (k INT)").executeUpdate()
      conn.prepareStatement("INSERT INTO dim VALUES (45), (0)").executeUpdate()
      withTempView("jdbc_side") {
        spark.read.jdbc(url, "dim", new Properties()).createOrReplaceTempView("jdbc_side")
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          assertHintedRewrite(
            "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN jdbc_side t ON bf4.c4 = t.k")
        }
      }
    } finally {
      conn.prepareStatement("DROP TABLE dim").executeUpdate()
      conn.close()
    }
  }

  test("RUNTIME_FILTER hint takes a side with a Python UDF its Bloom filter does not read") {
    import IntegratedUDFTestUtils._
    assume(shouldTestPythonUDFs)
    val pythonUDF = TestPythonUDF("bf_python_udf")
    registerTestUDF(pythonUDF, spark)
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The Bloom filter's subquery reads only what its key needs of the hinted side.
      assertHintedRewrite("SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN " +
        "(SELECT c2, bf_python_udf(a2) AS x FROM bf2) t ON bf4.c4 = t.c2")
      // A filter of the hinted side is part of it.
      val filtered = "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN " +
        "(SELECT c2 FROM bf2 WHERE bf_python_udf(a2) IS NOT NULL) t ON bf4.c4 = t.c2"
      assertNoBloomFilters(filtered)
      assertHintNotApplied(filtered,
        "the hinted side contains a Python UDF, Python UDTF or external UDF")
      // By the time that an IN subquery is joined with the outer query, the UDF is in an
      // operator of its own, which the Bloom filter's subquery does not read either.
      val hoisted = "SELECT * FROM bf1 WHERE c1 IN (SELECT /*+ RUNTIME_FILTER(t) */ t.x " +
        "FROM bf4 JOIN (SELECT c2, bf_python_udf(a2) AS x FROM bf2) t ON bf4.c4 = t.c2)"
      assert(bloomFilterApplicationSideKeys(hoisted).contains("c4"))
      assert(hintWarnings(hoisted).isEmpty)
    }
  }

  test("RUNTIME_FILTER hint adds no Bloom filter that can fail in place of a dropped DPP filter") {
    import IntegratedUDFTestUtils._
    assume(shouldTestPythonUDFs)
    val pythonUDF = TestPythonUDF("bf_python_udf")
    registerTestUDF(pythonUDF, spark)
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ANSI_ENABLED.key -> "true") {
      // DPP declines the hint for the Python UDF of the hinted side and adds an `onlyInBroadcast`
      // filter of its own, which physical planning drops without a broadcast to reuse.
      def query(key: String): String = "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf5part p JOIN " +
        s"(SELECT c2, bf_python_udf(a2) AS x FROM bf2 WHERE a2 = 62) t ON $key = t.c2"
      assert(hasDynamicPruning(query("p.f5")))
      assertRewroteWithBloomFilter(query("p.f5"))
      // On the partition column, a Bloom filter would evaluate the narrowing cast on every
      // partition, before the data filters (SPARK-60809).
      val narrowed = query("CAST(p.f5 AS TINYINT)")
      assert(hasDynamicPruning(narrowed))
      assertNoBloomFilters(narrowed)
      assertHintNotApplied(narrowed, "a Bloom filter pushed down on the join key could fail")
    }
  }

  test("RUNTIME_FILTER hint adds no runtime filter on a join key that can fail") {
    withTable("bf_fail_part", "bf_fail_dim") {
      // Every row of the partition `f = 200` has `a >= 100`.
      spark.range(30).selectExpr("id + CASE WHEN id < 20 THEN 0 ELSE 100 END AS a",
        "CASE WHEN id < 10 THEN 1 WHEN id < 20 THEN 2 ELSE 200 END AS f")
        .write.partitionBy("f").saveAsTable("bf_fail_part")
      sql("SELECT CAST(c AS TINYINT) AS c FROM VALUES (0), (1), (2), (0), (1) AS v(c)")
        .write.saveAsTable("bf_fail_dim")
      // On the partition column, a runtime filter would evaluate the narrowing cast on every
      // partition, before the data filter that leaves out the partition it overflows on
      // (SPARK-60809). Pushdown takes it into each branch of a union too, with its key rewritten
      // for the branch.
      Seq(
        ("bf_fail_part p JOIN bf_fail_dim t ON CAST(p.f AS TINYINT) = t.c WHERE p.a < 50", 30, 1),
        ("(SELECT * FROM bf_fail_part WHERE a < 50 UNION ALL SELECT * FROM bf_fail_part " +
          "WHERE a < 40) p JOIN bf_fail_dim t ON CAST(p.f AS TINYINT) = t.c", 60, 2)
      ).foreach { case (join, numRows, numScans) =>
        val unhinted = s"SELECT * FROM $join"
        val query = s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM $join"
        Seq("true", "false").foreach { dpp =>
          Seq("true", "false").foreach { aqe =>
            withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
                SQLConf.ANSI_ENABLED.key -> "true",
                SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> dpp,
                SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
              assert(!hasDynamicPruning(query))
              assertNoBloomFilters(query)
              assertHintNotApplied(query,
                "the join key could fail on rows the join does not read")
              assert(sql(unhinted).count() == numRows)
              checkAnswer(sql(query), sql(unhinted))
              // A key that cannot fail gets its hinted filter: a DPP filter, or a Bloom filter
              // with DPP off, on each scan.
              val safe = query.replace("CAST(p.f AS TINYINT) = t.c", "p.f = t.c")
              assert(hasDynamicPruning(safe) == dpp.toBoolean)
              assert(bloomFilterKeys(safe).size == (if (dpp.toBoolean) 0 else numScans))
              assert(hintWarnings(safe).isEmpty)
            }
          }
        }
      }
    }
  }

  test("RUNTIME_FILTER hint builds its filters from the hinted side as the join reads it") {
    withTable("bf_wf", "bf_wd", "bf_wdn", "bf_wd2") {
      spark.range(2000).selectExpr("id AS v", "CAST(1 + id % 100 AS INT) AS k",
        "CAST(1 + id % 100 AS INT) AS part").write.partitionBy("part").saveAsTable("bf_wf")
      // Only `p = 2` has a string that is not a number, and no row of `wd2` with `x = 1` has it.
      val wd = spark.range(1, 2000)
        .selectExpr("IF(id < 1999, CAST(id AS STRING), 'abc') AS s", "IF(id < 1999, 1, 2) AS p")
      wd.write.partitionBy("p").saveAsTable("bf_wd")
      wd.write.saveAsTable("bf_wdn")
      sql("SELECT * FROM VALUES (1, 1), (2, 0) AS v(p, x)").write.saveAsTable("bf_wd2")
      def dppKeys(query: String): Seq[String] =
        sql(query).queryExecution.optimizedPlan.flatMap {
          case Filter(condition, _) => condition.collect {
            case d: DynamicPruningSubquery => d.pruningKey.references.head.name
          }
          case _ => Nil
        }
      // The hinted `query` gets the Bloom filters on `bloomKeys` and returns what it returns
      // without `hint`, with `notApplied` as the reason if that hint is not applied.
      def check(
          query: String,
          hint: String,
          bloomKeys: Seq[String],
          notApplied: Option[String] = None): Unit = {
        assert(bloomFilterKeyNames(query).sorted == bloomKeys)
        notApplied match {
          case Some(reason) => assertHintNotApplied(query, reason)
          case None => assert(!hintWarnings(query).exists(_.contains("is not supported")))
        }
        // A count would prune the columns that the hinted side outputs.
        val expected = sql(query.replace(hint, "")).collect()
        assert(expected.nonEmpty)
        checkAnswer(sql(query), expected.toSeq)
      }
      val cannotFollow = "a DPP filter of a join outside the hinted side prunes it, which the " +
        "subquery of a hinted filter cannot follow"
      val d = "(SELECT CAST(s AS INT) AS k, p FROM bf_wdn) d"
      val z = "(SELECT p FROM bf_wd2 WHERE x = 1) z"
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ANSI_ENABLED.key -> "true") {
        // The join with `wd2` prunes `wd` with a DPP filter that reuses the broadcast of `wd2`.
        // The subquery of a hinted filter has no such broadcast, so it would read `p = 2` and
        // fail on the cast: of a DPP filter on `f.part`, and of a Bloom filter on `f.k`.
        Seq("f.part", "f.k").foreach { key =>
          val query = "SELECT /*+ RUNTIME_FILTER(d), BROADCAST(wd2), MERGE(d) */ f.v " +
            s"FROM bf_wf f JOIN (SELECT CAST(s AS INT) AS k, p FROM bf_wd) d ON $key = d.k " +
            "JOIN bf_wd2 wd2 ON d.p = wd2.p WHERE wd2.x = 1"
          Seq("true", "false").foreach { aqe =>
            withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
              assert(dppKeys(query) == Seq("p"))
              check(query, "RUNTIME_FILTER(d), ", Nil, Some(cannotFollow))
            }
          }
        }
        // The hinted join above prunes `d` with a Bloom filter, which pushdown brings into `d`
        // after the hinted filter of the join below is built.
        check("SELECT /*+ RUNTIME_FILTER(d), RUNTIME_FILTER(z) */ f.v FROM bf_wf f " +
          s"JOIN $d ON f.k = d.k JOIN $z ON d.p = z.p", "RUNTIME_FILTER(d), ", Seq("k", "p"))
        // The same for a hinted filter whose key reads both sides of a join in the pruned side:
        // pushdown leaves it in the condition of that join.
        check("SELECT /*+ RUNTIME_FILTER(d), RUNTIME_FILTER(z) */ f.v FROM bf_wf f " +
          s"JOIN bf_wf g ON f.v = g.v JOIN $d ON coalesce(f.k, g.k) = d.k JOIN $z ON d.p = z.p",
          "RUNTIME_FILTER(d), ", Seq("k", "k", "p"))
        // The DPP filter of the join within the hinted side `t` reuses the broadcast of `wd2`,
        // with its column `x`, which the Bloom filter does not read. Its subquery keeps the
        // column: without it, it would build another broadcast, lose the DPP filter and read
        // `p = 2`.
        Seq("true", "false").foreach { aqe =>
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
            val within = "SELECT /*+ RUNTIME_FILTER(t) */ f.v, t.x FROM bf_wf f JOIN " +
              "(SELECT /*+ BROADCAST(wd2) */ CAST(d.s AS INT) AS k, wd2.x FROM bf_wd d " +
              "JOIN bf_wd2 wd2 ON d.p = wd2.p WHERE wd2.x = 1 AND CAST(d.s AS INT) > 0) t " +
              "ON f.k = t.k"
            assert(dppKeys(within) == Seq("p"))
            check(within, "/*+ RUNTIME_FILTER(t) */ ", Seq("k"))
          }
        }
        // Pushdown moves the Bloom filter of the join within the hinted side `t` below the
        // aggregate, after the hinted filter on `f` is built: a Bloom filter on `f.k`, and a DPP
        // filter on `f.part`.
        val t = "(SELECT /*+ RUNTIME_FILTER(z) */ d.k FROM (SELECT k, p FROM " +
          s"(SELECT CAST(s AS INT) AS k, p FROM bf_wdn) GROUP BY k, p) d JOIN $z ON d.p = z.p) t"
        check(s"SELECT /*+ RUNTIME_FILTER(t) */ f.v FROM bf_wf f JOIN $t ON f.k = t.k",
          "/*+ RUNTIME_FILTER(t) */ ", Seq("k", "p"))
        val pruned = s"SELECT /*+ RUNTIME_FILTER(t) */ f.v FROM bf_wf f JOIN $t ON f.part = t.k"
        assert(dppKeys(pruned) == Seq("part"))
        check(pruned, "/*+ RUNTIME_FILTER(t) */ ", Seq("p"))
        // The join within the hinted side `t` loses its own hinted filter, and the filter that is
        // built from `t` does not keep a copy of it.
        check("SELECT /*+ RUNTIME_FILTER(t) */ g.v FROM bf_wf g JOIN " +
          "(SELECT /*+ RUNTIME_FILTER(d), BROADCAST(wd2), MERGE(d) */ f.k FROM bf_wf f " +
          "JOIN (SELECT CAST(s AS INT) AS k, p FROM bf_wd) d ON f.k = d.k " +
          "JOIN bf_wd2 wd2 ON d.p = wd2.p WHERE wd2.x = 1) t ON g.k = t.k",
          "/*+ RUNTIME_FILTER(t) */ ", Seq("k"), Some(cannotFollow))
        // The hint is decided in the IN subquery, before the outer query prunes `wd` with a DPP
        // filter of its own subquery.
        withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
          check("SELECT * FROM bf_wd2 o WHERE o.x = 1 AND o.p IN " +
            "(SELECT /*+ RUNTIME_FILTER(d) */ d.p FROM bf_wf f " +
            "JOIN (SELECT CAST(s AS INT) AS k, p FROM bf_wd) d ON f.k = d.k)",
            "/*+ RUNTIME_FILTER(d) */ ", Nil, Some(cannotFollow))
          // The same for the copy of the filter in a union branch with a literal key, whose hash
          // is a literal by then.
          check("SELECT * FROM bf_wd2 o WHERE o.x = 1 AND o.p IN " +
            "(SELECT /*+ RUNTIME_FILTER(d) */ d.p FROM " +
            "(SELECT k FROM bf_wf UNION ALL SELECT 7 AS k FROM bf_wf) f " +
            "JOIN (SELECT CAST(s AS INT) AS k, p FROM bf_wd) d ON f.k = d.k)",
            "/*+ RUNTIME_FILTER(d) */ ", Nil, Some(cannotFollow))
          // And when the key of the hinted side is an alias that the optimizer removes once the
          // subquery, whose output it was, is joined with the outer query.
          check("SELECT * FROM bf_wd2 o WHERE o.x = 1 AND o.p IN " +
            "(SELECT /*+ RUNTIME_FILTER(d) */ d.p FROM bf_wf f " +
            "JOIN (SELECT p AS p FROM bf_wd WHERE CAST(s AS INT) > 0) d ON f.k = d.p)",
            "/*+ RUNTIME_FILTER(d) */ ", Nil, Some(cannotFollow))
        }
        // Without that DPP filter, the hinted filter is built again on the key without the alias.
        check("SELECT * FROM bf_wd2 o WHERE o.p IN " +
          "(SELECT /*+ RUNTIME_FILTER(d) */ d.p FROM bf_wf f " +
          "JOIN (SELECT p AS p FROM bf_wd2 WHERE x = 1) d ON f.k = d.p)",
          "/*+ RUNTIME_FILTER(d) */ ", Seq("k"))
      }
      // A DPP filter of a join within the hinted side prunes the subquery as it does the join.
      withDppTestConf() {
        val within = "SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN " +
          "(SELECT /*+ BROADCAST(bf2) */ p.f5 FROM bf5part p JOIN bf2 ON p.f5 = bf2.c2 " +
          "WHERE bf2.a2 = 36) t ON bf4.c4 = t.f5"
        assert(dppKeys(within).nonEmpty)
        check(within, "/*+ RUNTIME_FILTER(t) */ ", Seq("c4"))
      }
      // Each join keeps its own filters, though the hinted side of the one within `t` and `t`
      // itself output the same column.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val nested = "SELECT /*+ RUNTIME_FILTER(t) */ bf4.a4 FROM bf4 JOIN " +
          "(SELECT /*+ RUNTIME_FILTER(bf1), RUNTIME_FILTER(bf3) */ bf1.c1 FROM bf1 " +
          "JOIN bf2 ON bf1.c1 = bf2.c2 JOIN bf3 ON bf1.c1 = bf3.c3) t ON bf4.c4 = t.c1"
        assert(bloomFilterKeyNames(nested).sorted == Seq("c1", "c2", "c4"))
        assert(hintWarnings(nested).isEmpty)
        checkAnswer(sql(nested), sql(nested.replaceAll("/\\*\\+ [A-Z_(), a-z0-9]+ \\*/ ", "")))
        // The join above filters `h2` on its row number, which another evaluation can assign
        // differently, so `h2` as the join below reads it is no source of a filter.
        val unstable = "SELECT /*+ RUNTIME_FILTER(h2), RUNTIME_FILTER(h1) */ * FROM bf1 a JOIN " +
          "(SELECT c2 AS k, row_number() OVER (PARTITION BY b2 ORDER BY a2) AS rn FROM bf2) h2 " +
          "ON a.c1 = h2.k JOIN bf3 h1 ON h2.rn = h1.c3"
        assert(bloomFilterKeyNames(unstable) == Seq("rn"))
        assertHintNotApplied(unstable,
          "the hinted side may produce different rows or join keys when evaluated again")
        // The join above loses that filter when a DPP filter prunes its own hinted side `h1`, so
        // `h2` is the source of a filter for the join below after all.
        val settled = "SELECT /*+ RUNTIME_FILTER(h2), RUNTIME_FILTER(h1), BROADCAST(z) */ * " +
          "FROM bf1 a JOIN (SELECT c2 AS k, row_number() OVER (PARTITION BY b2 ORDER BY a2) " +
          "AS rn FROM bf2) h2 ON a.c1 = h2.k JOIN bf5part h1 ON h2.rn = h1.c5 " +
          "JOIN bf3 z ON h1.f5 = z.c3 WHERE z.a3 = 10"
        assert(bloomFilterKeyNames(settled) == Seq("c1"))
        val warnings = hintWarnings(settled)
        assert(warnings.size == 1 && warnings.head.contains(cannotFollow))
        // In an IN subquery, the join below is left to the heuristics once the subquery is joined
        // with the outer query, like a join whose hinted side is declined to begin with.
        withSQLConf(
            SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000") {
          val hoisted = "SELECT * FROM bf4 WHERE bf4.c4 IN (SELECT /*+ RUNTIME_FILTER(h2), " +
            "RUNTIME_FILTER(h1) */ a.c1 FROM (SELECT * FROM bf1 WHERE a1 = 5) a JOIN " +
            "(SELECT c2 AS k, row_number() OVER (PARTITION BY b2 ORDER BY a2) AS rn FROM bf2) h2 " +
            "ON a.c1 = h2.k JOIN bf3 h1 ON h2.rn = h1.c3)"
          assert(bloomFilterKeyNames(hoisted).sorted == Seq("c4", "k", "rn"))
          assert(hintWarnings(hoisted).count(_.contains("is not supported")) == 1)
        }
        // In an IN subquery, the join with `r` prunes the hinted `d` with a DPP filter that finds
        // no broadcast to reuse, so the hinted DPP filter on `f` stays.
        val hinted = "SELECT * FROM bf4 WHERE bf4.c4 IN (SELECT /*+ RUNTIME_FILTER(d)%s */ f.c5 " +
          "FROM bf5part f JOIN bf5filtered d ON f.f5 = d.f5 " +
          "JOIN (SELECT c2 AS k FROM bf2 WHERE a2 > 1) r ON d.f5 = r.k)"
        assert(hasHintedDynamicPruning(hinted.format("")))
        assert(hintWarnings(hinted.format("")).isEmpty)
        // With the broadcast of `r`, the pass over the subquery reports the hint. It is not decided
        // again once the subquery is joined with the outer query: DPP adds no filter that honors
        // it then.
        val reported = hinted.format(", BROADCAST(r)")
        assert(!hasHintedDynamicPruning(reported))
        assert(hintWarnings(reported).count(_.contains("is not supported")) == 1)
        assertHintNotApplied(reported, cannotFollow)
      }
    }
  }

  test("RUNTIME_FILTER hint takes a materialized CTE whose definition is repeatable") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // A CTE that is not inlined is evaluated from its definition wherever it is read.
      assertHintedRewrite("WITH d AS MATERIALIZED (SELECT c2 FROM bf2 WHERE a2 < 60) " +
        "SELECT /*+ RUNTIME_FILTER(d) */ * FROM bf4 JOIN d ON bf4.c4 = d.c2")
      val unstable = "WITH d AS MATERIALIZED (SELECT c2 FROM bf2 WHERE rand() < 0.5) " +
        "SELECT /*+ RUNTIME_FILTER(d) */ * FROM bf4 JOIN d ON bf4.c4 = d.c2"
      assertNoBloomFilters(unstable)
      assertHintNotApplied(unstable,
        "the hinted side may produce different rows or join keys when evaluated again")
      val pruned = "WITH d AS MATERIALIZED (SELECT c2 FROM bf2 WHERE a2 < 60) " +
        "SELECT /*+ RUNTIME_FILTER(d) */ * FROM bf5part p JOIN d ON p.f5 = d.c2"
      assert(hasDynamicPruning(pruned))
      assert(!hintWarnings(pruned).exists(_.contains("is not supported")))
      checkAnswer(sql(pruned), sql(pruned.replace("/*+ RUNTIME_FILTER(d) */ ", "")))
    }
  }

  test("RUNTIME_FILTER hint in an IN subquery takes a materialized CTE of the outer query") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The subquery is optimized on its own first, without the definition of `d`, so the hint is
      // decided once the subquery is joined with the rest of the query.
      val query = "WITH d AS MATERIALIZED (SELECT c2 FROM bf2 WHERE a2 < 60) " +
        "SELECT * FROM bf4 WHERE bf4.c4 IN " +
        "(SELECT /*+ RUNTIME_FILTER(d) */ bf3.c3 FROM bf3 JOIN d ON bf3.c3 = d.c2)"
      assert(bloomFilterApplicationSideKeys(query).contains("c3"))
      assert(!hintWarnings(query).exists(_.contains("is not supported")))
      assertReturnsRows(query)
      checkAnswer(sql(query), sql(query.replace("/*+ RUNTIME_FILTER(d) */ ", "")))
      // Until then, the subquery adds no filter for the join either, next to which the hinted one
      // would be a second filter on the same key.
      val nested = "WITH x AS MATERIALIZED (SELECT b1 AS k FROM bf1) " +
        "SELECT * FROM bf4 WHERE bf4.c4 IN (SELECT /*+ RUNTIME_FILTER(d) */ bf3.c3 FROM bf3 " +
        "JOIN (SELECT bf2.c2 FROM bf2 JOIN x ON bf2.b2 = x.k WHERE bf2.a2 = 62) d " +
        "ON bf3.c3 = d.c2)"
      assert(bloomFilterKeyNames(nested).count(_ == "c3") == 1)
      assert(hintWarnings(nested).isEmpty)
      checkAnswer(sql(nested), sql(nested.replace("/*+ RUNTIME_FILTER(d) */ ", "")))
      // Nor does the subquery prune the hinted side with a DPP filter of this join, which the
      // outer query would then find there.
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false") {
        val prunable = "WITH x AS MATERIALIZED (SELECT b1 AS k FROM bf1) " +
          "SELECT * FROM bf4 WHERE bf4.c4 IN (SELECT /*+ RUNTIME_FILTER(d) */ bf2.c2 FROM bf2 " +
          "JOIN (SELECT p.f5 FROM bf5part p JOIN x ON p.b5 = x.k) d ON bf2.c2 = d.f5 " +
          "WHERE bf2.a2 = 62)"
        assert(!hasDynamicPruning(prunable))
        assert(bloomFilterKeyNames(prunable).contains("c2"))
        assert(hintWarnings(prunable).isEmpty)
      }
      // The same when the hinted side reads the CTE in a subquery of one of its expressions.
      val inExpression = "WITH x AS MATERIALIZED (SELECT max(a1) AS m FROM bf1) " +
        "SELECT * FROM bf4 WHERE bf4.c4 IN (SELECT /*+ RUNTIME_FILTER(d) */ bf3.c3 FROM bf3 " +
        "JOIN (SELECT c2 FROM bf2 WHERE a2 < (SELECT m FROM x)) d ON bf3.c3 = d.c2)"
      assert(bloomFilterApplicationSideKeys(inExpression).contains("c3"))
      assert(hintWarnings(inExpression).isEmpty)
      checkAnswer(sql(inExpression), sql(inExpression.replace("/*+ RUNTIME_FILTER(d) */ ", "")))
      // A scalar subquery is planned on its own, so no pass decides its hint and its join gets no
      // Bloom filter.
      val scalar = "WITH d AS MATERIALIZED (SELECT c2 FROM bf2 WHERE a2 < 60) SELECT " +
        "(SELECT /*+ RUNTIME_FILTER(d) */ count(*) FROM bf3 JOIN d ON bf3.c3 = d.c2) AS n"
      assert(sql(scalar).queryExecution.optimizedPlan.collectWithSubqueries {
        case f: Filter if f.condition.exists(_.isInstanceOf[BloomFilterMightContain]) => f
      }.isEmpty)
      assert(hintWarnings(scalar).isEmpty)
    }
  }

  test("RUNTIME_FILTER hint takes local data, a range, literal rows and a command result") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      def query(source: String): String =
        s"SELECT /*+ RUNTIME_FILTER(t) */ * FROM bf4 JOIN ($source) t ON bf4.c4 = t.k"
      assertHintedRewrite(query("SELECT * FROM VALUES (45), (0) AS v(k)"))
      assertHintedRewrite(query("SELECT CAST(id AS INT) AS k FROM range(50)"))
      // A single row would be folded into a filter on `bf4`, leaving no join key.
      assertHintedRewrite(query("SELECT 45 AS k UNION ALL SELECT 0 AS k"))
      withTempView("command_side") {
        sql("SHOW TABLES").createOrReplaceTempView("command_side")
        assertRewroteWithBloomFilter(
          query("SELECT CAST(length(tableName) AS INT) AS k FROM command_side"))
      }
    }
  }

  test("DPP cleanup leaves the shared true literal untagged") {
    // Cleanup replaces a DPP filter it drops, here for the static partition filter, with a
    // literal, onto which the transform copies the filter's tags.
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      sql("SELECT /*+ RUNTIME_FILTER(bf2) */ * FROM bf5part p JOIN bf2 ON p.f5 = bf2.c2 " +
        "WHERE p.f5 = 20").queryExecution.optimizedPlan
      assert(Literal.TrueLiteral.getTagValue(DynamicPruning.HONORS_RUNTIME_FILTER_HINT).isEmpty)
    }
  }
}
