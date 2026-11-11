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

package org.apache.spark.sql.execution

import java.util.Collections

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.{SparkContext, SparkEnv, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.{Expressions, SortDirection, SortOrder,
  Transform}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLShuffleReadMetricsReporter,
  SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.functions.{array, broadcast, col, explode, rand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.storage.TempLocalBlockId

/**
 * Tests the ordered executor-side gather of `CollectLimitExec.doExecute`, which returns the child
 * partitions in partition index order, as `executeCollect` does.
 *
 * In local mode all map outputs are local and arrive in map index order, so the reordering itself
 * is covered by the `OrderedGatherIterator` tests below, which return map outputs in reverse, and
 * by `CollectLimitOrderedGatherClusterSuite`, which reads map outputs from several executors.
 */
class CollectLimitOrderedGatherSuite extends QueryTest with SharedSparkSession {
  import CollectLimitOrderedGatherSuite._

  /** Returns the `ShuffledRowRDD` that gathers the child partitions for `CollectLimitExec`. */
  private def gatherRDD(collectLimitRDD: RDD[InternalRow]): ShuffledRowRDD = {
    collectLimitRDD.dependencies.map(_.rdd) match {
      case Seq(shuffled: ShuffledRowRDD) => shuffled
      case other => fail(s"Expected a ShuffledRowRDD, found: $other")
    }
  }

  /** Runs `f` with the ordered gather enabled, and with `confs`. */
  private def withOrderedGather(confs: (String, String)*)(f: => Unit): Unit = {
    val orderedGatherConf = SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED.key -> "true"
    withSQLConf(orderedGatherConf +: confs: _*)(f)
  }

  /**
   * Runs `f` with `confs` set in the conf of the `SparkEnv`, which the shuffle writers read when
   * they are created.
   */
  private def withSparkEnvConf(confs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    val originals = confs.map { case (key, _) => key -> conf.getOption(key) }
    confs.foreach { case (key, value) => conf.set(key, value) }
    try f finally {
      originals.foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None) => conf.remove(key)
      }
    }
  }

  /** Returns the `CollectLimitExec` that `df` is planned with. Requires AQE to be disabled. */
  private def plannedCollectLimit(df: Dataset[_]): CollectLimitExec = {
    df.queryExecution.executedPlan match {
      case collectLimit: CollectLimitExec => collectLimit
      case other => fail(s"Expected a CollectLimitExec, found:\n$other")
    }
  }

  /**
   * Returns the `CollectLimitExec` that `df` was executed with. With AQE, this is the one of the
   * final plan, which is planned again with the completed query stages.
   */
  private def executedCollectLimit(df: Dataset[_]): CollectLimitExec = {
    val executedPlan = df.queryExecution.executedPlan match {
      // With AQE, the final plan runs as a result query stage.
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan match {
        case stage: QueryStageExec => stage.plan
        case plan => plan
      }
      case plan => plan
    }
    executedPlan match {
      case collectLimit: CollectLimitExec => collectLimit
      case other => fail(s"Expected a CollectLimitExec, found:\n$other")
    }
  }

  /**
   * Checks that `CollectLimitExec` reads `child`, the plan of `query`, with the ordered gather and
   * returns the ids of `expected` that `limit` and `offset` select.
   */
  private def checkOrderedGather(
      query: String, child: SparkPlan, limit: Int, offset: Int, expected: Seq[Long]): Unit = {
    val rdd = CollectLimitExec(limit, child, offset).execute()
    assert(gatherRDD(rdd).isOrderedGather)
    val expectedIds = if (limit < 0) expected.drop(offset) else expected.slice(offset, limit)
    assert(rdd.map(_.getLong(0)).collect().toSeq === expectedIds,
      s"query: $query, limit: $limit, offset: $offset")
  }

  for (aqe <- Seq(false, true); bypassMergeThreshold <- Seq(0, 200)) {
    test("SPARK-61296: ordered gather returns the child partitions in partition order " +
      s"(AQE = $aqe, bypassMergeThreshold = $bypassMergeThreshold)") {
      val ids = (0L until 80L).filter(_ % 7 != 0)
      val queries = Seq(
        "SELECT id FROM range(0, 80, 1, 8) WHERE id % 7 != 0 ORDER BY id DESC" -> ids.reverse,
        // Without ORDER BY, the partition order is the order of `range`.
        "SELECT id FROM range(0, 80, 1, 8) WHERE id % 7 != 0" -> ids)
      val limitsAndOffsets = Seq((11, 0), (31, 19), (100, 60), (-1, 19), (-1, 100))
      // With a bypassMergeThreshold of 0, the sort-based shuffle writer spills every 7 rows, to
      // check that it keeps the order within map outputs. With 200, the bypass writer, which never
      // spills, writes the map outputs instead.
      withSparkEnvConf(
        "spark.shuffle.sort.bypassMergeThreshold" -> bypassMergeThreshold.toString,
        "spark.shuffle.spill.numElementsForceSpillThreshold" -> "7") {
        withOrderedGather(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "8",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
          for {
            (query, expected) <- queries
            child = spark.sql(query).queryExecution.executedPlan
            (limit, offset) <- limitsAndOffsets
          } checkOrderedGather(query, child, limit, offset, expected)
        }
      }
    }
  }

  test("ordered gather applies at any LIMIT when the child may be ordered") {
    val child = spark.range(0, 80, 1, 8).queryExecution.executedPlan
    // (enabled, childMayBeOrdered, limit). The top-K threshold does not matter.
    for {
      enabled <- Seq(true, false)
      childMayBeOrdered <- Seq(true, false)
      limit <- Seq(20, 60, -1)
    } {
      withSQLConf(
        SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED.key -> enabled.toString,
        SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "50") {
        val collectLimit = CollectLimitExec(limit, child, offset = 10, childMayBeOrdered)
        val gather = gatherRDD(collectLimit.execute())
        val ordered = enabled && childMayBeOrdered
        val clue = s"enabled: $enabled, childMayBeOrdered: $childMayBeOrdered, limit: $limit"
        assert(gather.isOrderedGather === ordered, clue)
        // The plan only shows the ordered gather metrics when the ordered gather runs.
        assert(collectLimit.metrics.contains("orderedGatherNumBatches") === ordered, clue)
      }
    }
  }

  test("childMayBeOrdered is not shown in the explain output") {
    val child = spark.range(0, 80, 1, 8).queryExecution.executedPlan
    assert(CollectLimitExec(20, child, 5, childMayBeOrdered = false).simpleString(100) ===
      CollectLimitExec(20, child, 5, childMayBeOrdered = true).simpleString(100))
  }

  for (aqe <- Seq(false, true)) {
    test("CollectLimitExec knows when the rows of its child come in no particular order " +
      s"(AQE = $aqe)") {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        withTempPath { path =>
          spark.range(0, 80, 1, 8).write.parquet(path.getCanonicalPath)
          val table = spark.read.parquet(path.getCanonicalPath)
          val sorted = table.orderBy(col("id").desc)
          val cached = sorted.select((col("id") + 1).as("id")).cache()
          try {
            for ((name, df, mayBeOrdered) <- Seq(
                ("filtered table", table.filter(col("id") % 7 =!= 0).offset(5), false),
                // Each of the LIMIT and OFFSET combinations that `SpecialLimits` plans.
                ("table with a limit", table.limit(20), false),
                ("table with an offset and a limit", table.offset(5).limit(20), false),
                ("table with a limit and an offset", table.limit(20).offset(5), false),
                ("aggregated table", table.groupBy((col("id") % 3).as("k")).count().offset(1),
                  false),
                ("table with explode", table.select(explode(array(col("id"), col("id")))).offset(5),
                  false),
                ("union of tables", table.union(table).offset(5), false),
                ("join of tables", table.join(table.select(col("id")), "id").offset(5), false),
                ("sorted table", sorted.offset(5), true),
                ("sorted table with explode",
                  sorted.select(explode(array(col("id"), col("id")))).limit(20), true),
                ("sorted table with a non-deterministic filter",
                  sorted.filter(rand() * col("id") >= 0).limit(20), true),
                // Any child of a union or a join can order the rows. A sort below a join is
                // removed by `EliminateSorts`, but range partitioning is kept.
                ("union with a sorted table", table.union(sorted).offset(5), true),
                ("join with a range partitioned table",
                  table.repartitionByRange(8, col("id")).join(broadcast(table), "id").offset(5),
                  true),
                ("range partitioned table", table.repartitionByRange(8, col("id")).offset(5),
                  true),
                ("cached sorted table", cached.offset(5), true),
                ("range", spark.range(0, 80, 1, 8).offset(5), true))) {
              // With AQE, the final plan is planned again with the completed query stages.
              df.collect()
              assert(executedCollectLimit(df).childMayBeOrdered === mayBeOrdered, name)
            }
          } finally {
            cached.unpersist()
          }
        }
      }
    }
  }

  for (aqe <- Seq(false, true)) {
    test(s"CollectLimitExec keeps the order that a DSv2 scan reports (AQE = $aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
        val catalog = spark.sessionState.catalogManager.catalog("testcat")
          .asInstanceOf[InMemoryTableCatalog]
        try {
          val byId = Expressions.sort(Expressions.column("id"), SortDirection.ASCENDING)
          for ((name, ordering, mayBeOrdered) <- Seq(
              ("ordered", Array(byId), true),
              ("unordered", Array.empty[SortOrder], false))) {
            catalog.createTable(Identifier.of(Array("ns"), name),
              Array(Column.create("id", LongType)), Array.empty[Transform],
              Collections.emptyMap[String, String](), Distributions.unspecified(), ordering,
              None, None, numRowsPerSplit = 10)
            spark.range(0, 80, 1, 8).writeTo(s"testcat.ns.$name").append()
            val df = spark.table(s"testcat.ns.$name").offset(5)
            // With AQE, the final plan is planned again with the completed query stages.
            df.collect()
            assert(executedCollectLimit(df).childMayBeOrdered === mayBeOrdered, name)
          }
        } finally {
          spark.sessionState.catalogManager.reset()
        }
      }
    }
  }

  test("SPARK-61296: ordered gather keeps the order of a sorted child that is not planned " +
    "as a top-K sort") {
    withOrderedGather(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
      // Only a LIMIT or OFFSET over a sort, or over a projection of a sort, is planned as a top-K
      // sort. A deterministic filter would be pushed below the sort, so this one is not, and keeps
      // every row without `OptimizeRand` removing it. An OFFSET without a LIMIT is never planned as
      // a top-K sort.
      val sorted = spark.range(0, 80, 1, 8).orderBy(col("id").desc)
      val ids = (0L until 80L).reverse
      for ((df, expected) <- Seq(
          sorted.select(explode(array(col("id"), col("id")))).offset(5).limit(10) ->
            ids.flatMap(id => Seq(id, id)).slice(5, 15),
          sorted.filter(rand() * col("id") >= 0).offset(5).limit(10) -> ids.slice(5, 15),
          sorted.offset(70) -> ids.drop(70))) {
        val collectLimit = plannedCollectLimit(df)
        assert(collectLimit.childMayBeOrdered)
        val rdd = collectLimit.execute()
        assert(gatherRDD(rdd).isOrderedGather)
        assert(rdd.map(_.getLong(0)).collect().toSeq === expected)
      }
    }
  }

  test("ordered gather disallows shuffle merging") {
    withOrderedGather() {
      val sc = spark.sparkContext
      val child = spark.range(0, 10, 1, 4).queryExecution.executedPlan
      val gather = gatherRDD(CollectLimitExec(5, child).execute())
      // Local mode never allows push-based shuffle merging, so allow it to check that the ordered
      // gather disallows it. Merged map outputs lose their map indexes.
      gather.dependency.setShuffleMergeAllowed(true)
      gather.enableOrderedGather(
        OrderedGatherOptions(spark.sessionState.conf, 4, 5), OrderedGatherMetrics(sc))
      assert(!gather.dependency.shuffleMergeAllowed)
    }
  }

  test("ordered gather requires a single-partition shuffle") {
    val sc = spark.sparkContext
    val shuffled = new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        sc.parallelize(Seq.empty[InternalRow], 4),
        Seq(AttributeReference("id", LongType)()),
        RoundRobinPartitioning(2),
        new UnsafeRowSerializer(1),
        SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sc)),
      SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sc))
    val e = intercept[IllegalArgumentException] {
      shuffled.enableOrderedGather(
        OrderedGatherOptions(spark.sessionState.conf, 4, 5), OrderedGatherMetrics(sc))
    }
    assert(e.getMessage.contains("The ordered gather requires a single-partition shuffle"))
    assert(!shuffled.isOrderedGather)
  }

  /** Collects the long values of `df` through `toLocalIterator`. */
  private def localIteratorCollect(df: DataFrame): Seq[Long] =
    df.toLocalIterator().asScala.map(_.getLong(0)).toList

  /** Collects the long values of `df` through the Arrow batches of `toArrowBatchRdd`. */
  private def arrowCollect(df: DataFrame): Seq[Long] = {
    val batches = df.toArrowBatchRdd.collect()
    ArrowConverters.fromBatchIterator(
      batches.iterator,
      df.schema,
      spark.sessionState.conf.sessionLocalTimeZone,
      errorOnDuplicatedFieldNames = true,
      largeVarTypes = false,
      TaskContext.empty()).map(_.getLong(0)).toList
  }

  test("SPARK-61296: ordered gather on the toLocalIterator and Arrow collect paths") {
    withOrderedGather(
      SQLConf.SHUFFLE_PARTITIONS.key -> "8",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val query = "SELECT id FROM range(0, 80, 1, 8) ORDER BY id DESC OFFSET 19"
      val expected = (0L until 80L).reverse.drop(19)
      assert(localIteratorCollect(spark.sql(query)) === expected)
      assert(arrowCollect(spark.sql(query)) === expected)
    }
  }

  for (aqe <- Seq(false, true)) {
    test(s"ordered gather metrics on the Arrow collect path (AQE = $aqe)") {
      withOrderedGather(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "8",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
        val df = spark.sql("SELECT id FROM range(0, 80, 1, 8) ORDER BY id DESC OFFSET 19")
        assert(arrowCollect(df) === (0L until 80L).reverse.drop(19))
        assert(executedCollectLimit(df).metrics("orderedGatherNumBatches").value > 0)
      }
    }
  }

  /** Returns the result of `f` and the number of shuffle block fetchers it creates. */
  private def countShuffleBlockFetchers[T](f: => T): (T, Int) = {
    val appender = new LogAppender("shuffle block fetchers")
    val fetcherLogger = "org.apache.spark.storage.ShuffleBlockFetcherIterator"
    var result: Option[T] = None
    withLogAppender(appender, loggerNames = Seq(fetcherLogger)) {
      result = Some(f)
    }
    // Every fetcher logs once which blocks it gets.
    val numFetchers = appender.loggingEvents.count(
      _.getMessage.getFormattedMessage.contains("blocks including"))
    assert(numFetchers > 0, s"No fetcher logs from $fetcherLogger. Was the class renamed or moved?")
    (result.get, numFetchers)
  }

  test("ordered gather reads in batches of maxBatchPartitions") {
    // Without a shuffle below the OFFSET, the gather is the only shuffle read. The ordered gather
    // creates a fetcher per batch, so it reads the 8 map outputs with 3 fetchers.
    for ((enabled, expectedFetchers) <- Seq((true, 3), (false, 1))) {
      withSQLConf(
        SQLConf.COLLECT_LIMIT_ORDERED_GATHER_ENABLED.key -> enabled.toString,
        SQLConf.COLLECT_LIMIT_ORDERED_GATHER_MAX_BATCH_PARTITIONS.key -> "3") {
        val df = spark.sql("SELECT id FROM range(0, 80, 1, 8) OFFSET 19")
        val (values, fetchers) = countShuffleBlockFetchers(localIteratorCollect(df))
        assert(values === (0L until 80L).drop(19))
        assert(fetchers === expectedFetchers, s"ordered gather enabled: $enabled")
      }
    }
  }

  /** Returns the spool files of the ordered gather that exist in the local block manager. */
  private def spoolFiles(): Set[TempLocalBlockId] = {
    SparkEnv.get.blockManager.diskBlockManager.getAllBlocks().collect {
      case blockId: TempLocalBlockId => blockId
    }.toSet
  }

  /**
   * Runs an [[OrderedGatherIterator]] in a task over map outputs with `mapSizes` rows. See
   * `syntheticBatch` for `arrivalOrder` and `listEmpty`.
   *
   * @param limit the number of rows to return, or -1 to return all of them.
   * @param maxBatchSize the largest number of map outputs to read in one batch.
   */
  private def orderedGather(
      mapSizes: Seq[Int],
      limit: Int,
      maxBatchSize: Int = 100,
      arrivalOrder: Seq[Int] => Seq[Int] = _.reverse,
      listEmpty: Boolean = false): OrderedGatherResult = {
    val spoolFilesBefore = spoolFiles()
    val metrics = OrderedGatherMetrics(spark.sparkContext)
    val (values, batches, spooledBytes) = runInTask(spark.sparkContext) { context =>
      val batches = mutable.ArrayBuffer.empty[(Int, Int)]
      val gathered = new OrderedGatherIterator(
        context,
        testOptions(mapSizes.length, limit, maxBatchSize),
        metrics,
        new UnsafeRowSerializer(1),
        (start, end) => {
          batches += ((start, end))
          syntheticBatch(mapSizes, arrivalOrder, listEmpty)(start, end)
        })
      (gathered.map(_.getLong(0)).toList, batches.toList, context.taskMetrics().diskBytesSpilled)
    }
    assert(metrics.numBatches.value === batches.length)
    assert(metrics.spoolSize.value === spooledBytes)
    // The spools are deleted when the task completes, before its result is returned.
    OrderedGatherResult(values, batches, spooledBytes, spoolFiles() -- spoolFilesBefore)
  }

  /** Returns the values of map outputs with `mapSizes` rows, in map index order. */
  private def mapOutputValues(mapSizes: Seq[Int]): Seq[Long] = {
    mapSizes.zipWithIndex.flatMap { case (size, mapIndex) =>
      (0 until size).map(mapOutputValue(mapIndex, _))
    }
  }

  test("OrderedGatherOptions takes its batch sizes from the session conf") {
    withSQLConf(
      SQLConf.LIMIT_INITIAL_NUM_PARTITIONS.key -> "3",
      // As in `executeTake`, the scale-up factor is at least 2.
      SQLConf.LIMIT_SCALE_UP_FACTOR.key -> "1",
      SQLConf.COLLECT_LIMIT_ORDERED_GATHER_MAX_BATCH_PARTITIONS.key -> "7") {
      assert(OrderedGatherOptions(spark.sessionState.conf, numMaps = 20, limit = 5) ===
        OrderedGatherOptions(
          numMaps = 20, limit = 5, initialBatchSize = 3, scaleUpFactor = 2, maxBatchSize = 7))
    }
  }

  test("OrderedGatherIterator returns map outputs in map index order") {
    val mapSizes = Seq(3, 0, 2, 4, 1, 0, 5)
    val result = orderedGather(mapSizes, limit = -1, maxBatchSize = 4)
    assert(result.values === mapOutputValues(mapSizes))
    assert(result.batches === Seq((0, 4), (4, 7)))
    // All but the first map output of each batch arrive early, so they are spooled.
    assert(result.spooledBytes > 0)
    assert(result.spoolFilesLeft.isEmpty)
  }

  test("OrderedGatherIterator returns map outputs that arrive in order without spooling them") {
    val mapSizes = Seq(3, 0, 2, 4, 1, 0, 5)
    // Empty map outputs, whether left out or returned without rows, do not hold up the others.
    for (listEmpty <- Seq(false, true)) {
      val result = orderedGather(
        mapSizes, limit = -1, maxBatchSize = 4, arrivalOrder = identity, listEmpty = listEmpty)
      assert(result.values === mapOutputValues(mapSizes))
      assert(result.spooledBytes === 0)
    }
  }

  test("OrderedGatherIterator grows its batches like executeTake and stops at the limit") {
    // The first batch returns 2 rows from 1 map output. For the 7 rows left, the next batch would
    // read ceil(1.5 * 7 * 1 / 2) = 6 map outputs, but is capped at 1 * scaleUpFactor = 4.
    val mapSizes = Seq.fill(20)(2)
    val result = orderedGather(mapSizes, limit = 9)
    assert(result.values === mapOutputValues(mapSizes).take(9))
    assert(result.batches === Seq((0, 1), (1, 5)))

    // Without any rows so far, the batch grows by scaleUpFactor.
    val sparseMapSizes = Seq(0, 0, 3, 3, 3, 3)
    val sparseResult = orderedGather(sparseMapSizes, limit = 4)
    assert(sparseResult.values === mapOutputValues(sparseMapSizes).take(4))
    assert(sparseResult.batches === Seq((0, 1), (1, 5)))
  }

  test("OrderedGatherIterator deletes the spools that are left at the limit") {
    // The second batch reads map outputs [1, 4) in reverse, so the limit is reached while map
    // outputs 2 and 3 are still spooled.
    val mapSizes = Seq(1, 5, 5, 5)
    val result = orderedGather(mapSizes, limit = 4)
    assert(result.values === mapOutputValues(mapSizes).take(4))
    assert(result.batches === Seq((0, 1), (1, 4)))
    assert(result.spooledBytes > 0)
    assert(result.spoolFilesLeft.isEmpty)
  }

  test("OrderedGatherIterator only spools the rows that can still be returned") {
    // The second batch reads map outputs [1, 3) in reverse. Only 3 rows of map output 2 can still
    // be returned, so it spools as much as a map output with only these 3 rows.
    val result = orderedGather(Seq(1, 50, 50), limit = 4)
    assert(result.values === mapOutputValues(Seq(1, 50, 50)).take(4))
    assert(result.batches === Seq((0, 1), (1, 3)))
    assert(result.spooledBytes > 0)
    assert(result.spooledBytes === orderedGather(Seq(1, 50, 3), limit = 4).spooledBytes)
    assert(result.spoolFilesLeft.isEmpty)
  }

  test("OrderedGatherIterator does not spool rows behind the spooled rows of lower map outputs") {
    // The second batch reads map outputs [1, 4), with map output 1 last. Map output 2 arrives early
    // with the 3 rows that can still be returned, so none of map output 3 can be returned, and it
    // is not spooled at all, as if it were empty.
    val arrivalOrder = (mapIndexes: Seq[Int]) => mapIndexes.tail :+ mapIndexes.head
    val result = orderedGather(Seq(1, 50, 50, 50), limit = 4, arrivalOrder = arrivalOrder)
    assert(result.values === mapOutputValues(Seq(1, 50, 50, 50)).take(4))
    assert(result.batches === Seq((0, 1), (1, 4)))
    assert(result.spooledBytes > 0)
    assert(result.spooledBytes ===
      orderedGather(Seq(1, 50, 50, 0), limit = 4, arrivalOrder = arrivalOrder).spooledBytes)
    assert(result.spoolFilesLeft.isEmpty)
  }

  test("OrderedGatherIterator only caps a spool by the spooled rows of lower map outputs") {
    // The second batch reads map outputs [1, 4) in reverse. Map output 3 is spooled first, but its
    // rows come after those of map output 2, so they must not lower the rows that map output 2
    // spools: all 3 rows of map output 2 are returned.
    val mapSizes = Seq(1, 1, 3, 3)
    val result = orderedGather(mapSizes, limit = 6)
    assert(result.batches === Seq((0, 1), (1, 4)))
    assert(result.values === mapOutputValues(mapSizes).take(6))
    assert(result.spoolFilesLeft.isEmpty)
  }

  /**
   * Runs an [[OrderedGatherIterator]] in a task over a single batch of `numMaps` map outputs that
   * lists `listed` and returns `returned`, a row each. Returns the error that the task fails with,
   * and checks that it leaves no spools behind.
   */
  private def orderedGatherError(numMaps: Int, listed: Seq[Int], returned: Seq[Int]): String = {
    val spoolFilesBefore = spoolFiles()
    val metrics = OrderedGatherMetrics(spark.sparkContext)
    val message = intercept[SparkException] {
      runInTask(spark.sparkContext) { context =>
        val readMapOutputs = (_: Int, _: Int) => new MapOutputBatch(
          listed.toArray,
          returned.iterator.map(mapIndex => (mapIndex, longRows(Iterator(mapIndex.toLong)))))
        new OrderedGatherIterator(context, testOptions(numMaps, limit = -1, numMaps), metrics,
          new UnsafeRowSerializer(1), readMapOutputs).length
      }
    }.getMessage
    // A failed task deletes its spools before its failure is reported.
    assert((spoolFiles() -- spoolFilesBefore).isEmpty, s"Spools left after the failure: $message")
    message
  }

  test("OrderedGatherIterator fails on unexpected or missing map outputs") {
    assert(orderedGatherError(numMaps = 2, listed = Seq(0, 1), returned = Seq(1, 1))
      .contains("Unexpected map output 1 when reading map outputs [0, 2)"))
    assert(orderedGatherError(numMaps = 2, listed = Seq(0), returned = Seq(1))
      .contains("Unexpected map output 1 when reading map outputs [0, 2)"))
    assert(orderedGatherError(numMaps = 2, listed = Seq(1), returned = Seq(1, 0))
      .contains("Unexpected map output 0 after reading map outputs [0, 2)"))
    assert(orderedGatherError(numMaps = 2, listed = Seq(0, 2), returned = Seq(0))
      .contains("Map output 2 to read is not in the map outputs [0, 2)"))
    // Map output 0 is listed but never returned, so the rows of map output 1 would be returned
    // without the rows of map output 0.
    assert(orderedGatherError(numMaps = 2, listed = Seq(0, 1), returned = Seq(1))
      .contains("Map output 0 was not returned when reading map outputs [0, 2)"))
  }
}

/**
 * Helpers of `CollectLimitOrderedGatherSuite`. The ones that run in tasks are here, because the
 * suite is not serializable.
 */
private object CollectLimitOrderedGatherSuite {

  /** The result of `CollectLimitOrderedGatherSuite.orderedGather`. */
  case class OrderedGatherResult(
      values: Seq[Long],
      batches: Seq[(Int, Int)],
      spooledBytes: Long,
      spoolFilesLeft: Set[TempLocalBlockId])

  /** Runs `f` in a single task and returns its result. */
  def runInTask[T: ClassTag](sc: SparkContext)(f: TaskContext => T): T = {
    sc.parallelize(Seq(0), 1).map(_ => f(TaskContext.get())).collect().head
  }

  /** Returns the options of the `OrderedGatherIterator` tests. */
  def testOptions(numMaps: Int, limit: Int, maxBatchSize: Int): OrderedGatherOptions = {
    OrderedGatherOptions(numMaps = numMaps, limit = limit, initialBatchSize = 1,
      scaleUpFactor = 4, maxBatchSize = maxBatchSize)
  }

  /** The value of row `i` of a synthetic map output. */
  def mapOutputValue(mapIndex: Int, i: Int): Long = mapIndex * 100L + i

  /** Returns rows that each hold one of `values`. */
  def longRows(values: Iterator[Long]): Iterator[InternalRow] = {
    val toUnsafe = UnsafeProjection.create(Array[DataType](LongType))
    values.map(value => toUnsafe(InternalRow(value)): InternalRow)
  }

  /**
   * Returns the map outputs `[start, end)`, where map output `m` has `mapSizes(m)` rows with the
   * values `mapOutputValue(m, i)`.
   *
   * @param arrivalOrder orders the map indexes of the map outputs as they arrive.
   * @param listEmpty whether the empty map outputs are listed too. They are then returned without
   *                  rows.
   */
  def syntheticBatch(
      mapSizes: Seq[Int],
      arrivalOrder: Seq[Int] => Seq[Int],
      listEmpty: Boolean)(start: Int, end: Int): MapOutputBatch = {
    val (withRows, empty) = (start until end).partition(mapSizes(_) > 0)
    val listed = if (listEmpty) withRows ++ empty else withRows
    val outputs = arrivalOrder(listed.sorted).iterator.map { mapIndex =>
      (mapIndex, longRows((0 until mapSizes(mapIndex)).iterator.map(mapOutputValue(mapIndex, _))))
    }
    new MapOutputBatch(listed.sorted.toArray, outputs)
  }
}
