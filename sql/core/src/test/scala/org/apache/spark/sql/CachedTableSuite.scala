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

import scala.collection.mutable.HashSet
import scala.concurrent.duration._

import org.apache.spark.CleanerListener
import org.apache.spark.executor.DataReadMethod._
import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Join, JoinStrategyHint, SHUFFLE_HASH}
import org.apache.spark.sql.execution.{RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.storage.StorageLevel.{MEMORY_AND_DISK_2, MEMORY_ONLY}
import org.apache.spark.util.{AccumulatorContext, Utils}

private case class BigData(s: String)

class CachedTableSuite extends QueryTest with SQLTestUtils with SharedSQLContext {
  import testImplicits._

  setupTestData()

  override def afterEach(): Unit = {
    try {
      spark.catalog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  def rddIdOf(tableName: String): Int = {
    val plan = spark.table(tableName).queryExecution.sparkPlan
    plan.collect {
      case InMemoryTableScanExec(_, _, relation) =>
        relation.cacheBuilder.cachedColumnBuffers.id
      case _ =>
        fail(s"Table $tableName is not cached\n" + plan)
    }.head
  }

  def isMaterialized(rddId: Int): Boolean = {
    val maybeBlock = sparkContext.env.blockManager.get(RDDBlockId(rddId, 0))
    maybeBlock.foreach(_ => sparkContext.env.blockManager.releaseLock(RDDBlockId(rddId, 0)))
    maybeBlock.nonEmpty
  }

  def isExpectStorageLevel(rddId: Int, level: DataReadMethod): Boolean = {
    val maybeBlock = sparkContext.env.blockManager.get(RDDBlockId(rddId, 0))
    val isExpectLevel = maybeBlock.forall(_.readMethod === level)
    maybeBlock.foreach(_ => sparkContext.env.blockManager.releaseLock(RDDBlockId(rddId, 0)))
    maybeBlock.nonEmpty && isExpectLevel
  }

  private def getNumInMemoryRelations(ds: Dataset[_]): Int = {
    val plan = ds.queryExecution.withCachedData
    var sum = plan.collect { case _: InMemoryRelation => 1 }.sum
    plan.transformAllExpressions {
      case e: SubqueryExpression =>
        sum += getNumInMemoryRelations(e.plan)
        e
    }
    sum
  }

  private def getNumInMemoryTablesRecursively(plan: SparkPlan): Int = {
    plan.collect {
      case InMemoryTableScanExec(_, _, relation) =>
        getNumInMemoryTablesRecursively(relation.cachedPlan) + 1
    }.sum
  }

  test("cache temp table") {
    withTempView("tempTable") {
      testData.select('key).createOrReplaceTempView("tempTable")
      assertCached(sql("SELECT COUNT(*) FROM tempTable"), 0)
      spark.catalog.cacheTable("tempTable")
      assertCached(sql("SELECT COUNT(*) FROM tempTable"))
      uncacheTable("tempTable")
    }
  }

  test("unpersist an uncached table will not raise exception") {
    assert(None == spark.sharedState.cacheManager.lookupCachedData(testData))
    testData.unpersist(blocking = true)
    assert(None == spark.sharedState.cacheManager.lookupCachedData(testData))
    testData.unpersist(blocking = false)
    assert(None == spark.sharedState.cacheManager.lookupCachedData(testData))
    testData.persist()
    assert(None != spark.sharedState.cacheManager.lookupCachedData(testData))
    testData.unpersist(blocking = true)
    assert(None == spark.sharedState.cacheManager.lookupCachedData(testData))
    testData.unpersist(blocking = false)
    assert(None == spark.sharedState.cacheManager.lookupCachedData(testData))
  }

  test("cache table as select") {
    withTempView("tempTable") {
      sql("CACHE TABLE tempTable AS SELECT key FROM testData")
      assertCached(sql("SELECT COUNT(*) FROM tempTable"))
      uncacheTable("tempTable")
    }
  }

  test("uncaching temp table") {
    testData.select('key).createOrReplaceTempView("tempTable1")
    testData.select('key).createOrReplaceTempView("tempTable2")
    spark.catalog.cacheTable("tempTable1")

    assertCached(sql("SELECT COUNT(*) FROM tempTable1"))
    assertCached(sql("SELECT COUNT(*) FROM tempTable2"))

    // Is this valid?
    uncacheTable("tempTable2")

    // Should this be cached?
    assertCached(sql("SELECT COUNT(*) FROM tempTable1"), 0)
  }

  test("too big for memory") {
    val data = "*" * 1000
    sparkContext.parallelize(1 to 200000, 1).map(_ => BigData(data)).toDF()
      .createOrReplaceTempView("bigData")
    spark.table("bigData").persist(StorageLevel.MEMORY_AND_DISK)
    assert(spark.table("bigData").count() === 200000L)
    spark.table("bigData").unpersist(blocking = true)
  }

  test("calling .cache() should use in-memory columnar caching") {
    spark.table("testData").cache()
    assertCached(spark.table("testData"))
    spark.table("testData").unpersist(blocking = true)
  }

  test("calling .unpersist() should drop in-memory columnar cache") {
    spark.table("testData").cache()
    spark.table("testData").count()
    spark.table("testData").unpersist(blocking = true)
    assertCached(spark.table("testData"), 0)
  }

  test("isCached") {
    spark.catalog.cacheTable("testData")

    assertCached(spark.table("testData"))
    assert(spark.table("testData").queryExecution.withCachedData match {
      case _: InMemoryRelation => true
      case _ => false
    })

    uncacheTable("testData")
    assert(!spark.catalog.isCached("testData"))
    assert(spark.table("testData").queryExecution.withCachedData match {
      case _: InMemoryRelation => false
      case _ => true
    })
  }

  test("SPARK-1669: cacheTable should be idempotent") {
    assume(!spark.table("testData").logicalPlan.isInstanceOf[InMemoryRelation])

    spark.catalog.cacheTable("testData")
    assertCached(spark.table("testData"))

    assertResult(1, "InMemoryRelation not found, testData should have been cached") {
      getNumInMemoryRelations(spark.table("testData"))
    }

    spark.catalog.cacheTable("testData")
    assertResult(0, "Double InMemoryRelations found, cacheTable() is not idempotent") {
      spark.table("testData").queryExecution.withCachedData.collect {
        case r: InMemoryRelation if r.cachedPlan.isInstanceOf[InMemoryTableScanExec] => r
      }.size
    }

    uncacheTable("testData")
  }

  test("read from cached table and uncache") {
    spark.catalog.cacheTable("testData")
    checkAnswer(spark.table("testData"), testData.collect().toSeq)
    assertCached(spark.table("testData"))

    uncacheTable("testData")
    checkAnswer(spark.table("testData"), testData.collect().toSeq)
    assertCached(spark.table("testData"), 0)
  }

  test("SELECT star from cached table") {
    sql("SELECT * FROM testData").createOrReplaceTempView("selectStar")
    spark.catalog.cacheTable("selectStar")
    checkAnswer(
      sql("SELECT * FROM selectStar WHERE key = 1"),
      Seq(Row(1, "1")))
    uncacheTable("selectStar")
  }

  test("Self-join cached") {
    val unCachedAnswer =
      sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key").collect()
    spark.catalog.cacheTable("testData")
    checkAnswer(
      sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key"),
      unCachedAnswer.toSeq)
    uncacheTable("testData")
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' SQL statement") {
    sql("CACHE TABLE testData")
    assertCached(spark.table("testData"))

    val rddId = rddIdOf("testData")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    sql("UNCACHE TABLE testData")
    assert(!spark.catalog.isCached("testData"), "Table 'testData' should not be cached")

    eventually(timeout(10.seconds)) {
      assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
    }
  }

  test("CACHE TABLE tableName AS SELECT * FROM anotherTable") {
    withTempView("testCacheTable") {
      sql("CACHE TABLE testCacheTable AS SELECT * FROM testData")
      assertCached(spark.table("testCacheTable"))

      val rddId = rddIdOf("testCacheTable")
      assert(
        isMaterialized(rddId),
        "Eagerly cached in-memory table should have already been materialized")

      uncacheTable("testCacheTable")
      eventually(timeout(10.seconds)) {
        assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
      }
    }
  }

  test("CACHE TABLE tableName AS SELECT ...") {
    withTempView("testCacheTable") {
      sql("CACHE TABLE testCacheTable AS SELECT key FROM testData LIMIT 10")
      assertCached(spark.table("testCacheTable"))

      val rddId = rddIdOf("testCacheTable")
      assert(
        isMaterialized(rddId),
        "Eagerly cached in-memory table should have already been materialized")

      uncacheTable("testCacheTable")
      eventually(timeout(10.seconds)) {
        assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
      }
    }
  }

  test("CACHE LAZY TABLE tableName") {
    sql("CACHE LAZY TABLE testData")
    assertCached(spark.table("testData"))

    val rddId = rddIdOf("testData")
    assert(
      !isMaterialized(rddId),
      "Lazily cached in-memory table shouldn't be materialized eagerly")

    sql("SELECT COUNT(*) FROM testData").collect()
    assert(
      isMaterialized(rddId),
      "Lazily cached in-memory table should have been materialized")

    uncacheTable("testData")
    eventually(timeout(10.seconds)) {
      assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
    }
  }

  private def assertStorageLevel(cacheOptions: String, level: DataReadMethod): Unit = {
    sql(s"CACHE TABLE testData OPTIONS$cacheOptions")
    assertCached(spark.table("testData"))
    val rddId = rddIdOf("testData")
    assert(isExpectStorageLevel(rddId, level))
  }

  test("SQL interface support storageLevel(DISK_ONLY)") {
    assertStorageLevel("('storageLevel' 'DISK_ONLY')", Disk)
  }

  test("SQL interface support storageLevel(DISK_ONLY) with invalid options") {
    assertStorageLevel("('storageLevel' 'DISK_ONLY', 'a' '1', 'b' '2')", Disk)
  }

  test("SQL interface support storageLevel(MEMORY_ONLY)") {
    assertStorageLevel("('storageLevel' 'MEMORY_ONLY')", Memory)
  }

  test("SQL interface cache SELECT ... support storageLevel(DISK_ONLY)") {
    withTempView("testCacheSelect") {
      sql("CACHE TABLE testCacheSelect OPTIONS('storageLevel' 'DISK_ONLY') SELECT * FROM testData")
      assertCached(spark.table("testCacheSelect"))
      val rddId = rddIdOf("testCacheSelect")
      assert(isExpectStorageLevel(rddId, Disk))
    }
  }

  test("SQL interface support storageLevel(Invalid StorageLevel)") {
    val message = intercept[IllegalArgumentException] {
      sql("CACHE TABLE testData OPTIONS('storageLevel' 'invalid_storage_level')")
    }.getMessage
    assert(message.contains("Invalid StorageLevel: INVALID_STORAGE_LEVEL"))
  }

  test("SQL interface support storageLevel(with LAZY)") {
    sql("CACHE LAZY TABLE testData OPTIONS('storageLevel' 'disk_only')")
    assertCached(spark.table("testData"))

    val rddId = rddIdOf("testData")
    assert(
      !isMaterialized(rddId),
      "Lazily cached in-memory table shouldn't be materialized eagerly")

    sql("SELECT COUNT(*) FROM testData").collect()
    assert(
      isMaterialized(rddId),
      "Lazily cached in-memory table should have been materialized")
    assert(isExpectStorageLevel(rddId, Disk))
  }

  test("InMemoryRelation statistics") {
    sql("CACHE TABLE testData")
    spark.table("testData").queryExecution.withCachedData.collect {
      case cached: InMemoryRelation =>
        val actualSizeInBytes = (1 to 100).map(i => 4 + i.toString.length + 4).sum
        assert(cached.stats.sizeInBytes === actualSizeInBytes)
    }
  }

  test("Drops temporary table") {
    testData.select('key).createOrReplaceTempView("t1")
    spark.table("t1")
    spark.catalog.dropTempView("t1")
    intercept[AnalysisException](spark.table("t1"))
  }

  test("Drops cached temporary table") {
    testData.select('key).createOrReplaceTempView("t1")
    testData.select('key).createOrReplaceTempView("t2")
    spark.catalog.cacheTable("t1")

    assert(spark.catalog.isCached("t1"))
    assert(spark.catalog.isCached("t2"))

    spark.catalog.dropTempView("t1")
    intercept[AnalysisException](spark.table("t1"))
    assert(!spark.catalog.isCached("t2"))
  }

  test("Clear all cache") {
    sql("SELECT key FROM testData LIMIT 10").createOrReplaceTempView("t1")
    sql("SELECT key FROM testData LIMIT 5").createOrReplaceTempView("t2")
    spark.catalog.cacheTable("t1")
    spark.catalog.cacheTable("t2")
    spark.catalog.clearCache()
    assert(spark.sharedState.cacheManager.isEmpty)

    sql("SELECT key FROM testData LIMIT 10").createOrReplaceTempView("t1")
    sql("SELECT key FROM testData LIMIT 5").createOrReplaceTempView("t2")
    spark.catalog.cacheTable("t1")
    spark.catalog.cacheTable("t2")
    sql("Clear CACHE")
    assert(spark.sharedState.cacheManager.isEmpty)
  }

  test("Ensure accumulators to be cleared after GC when uncacheTable") {
    sql("SELECT key FROM testData LIMIT 10").createOrReplaceTempView("t1")
    sql("SELECT key FROM testData LIMIT 5").createOrReplaceTempView("t2")

    spark.catalog.cacheTable("t1")
    spark.catalog.cacheTable("t2")

    sql("SELECT * FROM t1").count()
    sql("SELECT * FROM t2").count()
    sql("SELECT * FROM t1").count()
    sql("SELECT * FROM t2").count()

    val toBeCleanedAccIds = new HashSet[Long]

    val accId1 = spark.table("t1").queryExecution.withCachedData.collect {
      case i: InMemoryRelation => i.cacheBuilder.sizeInBytesStats.id
    }.head
    toBeCleanedAccIds += accId1

    val accId2 = spark.table("t1").queryExecution.withCachedData.collect {
      case i: InMemoryRelation => i.cacheBuilder.sizeInBytesStats.id
    }.head
    toBeCleanedAccIds += accId2

    val cleanerListener = new CleanerListener {
      def rddCleaned(rddId: Int): Unit = {}
      def shuffleCleaned(shuffleId: Int): Unit = {}
      def broadcastCleaned(broadcastId: Long): Unit = {}
      def accumCleaned(accId: Long): Unit = {
        toBeCleanedAccIds.synchronized { toBeCleanedAccIds -= accId }
      }
      def checkpointCleaned(rddId: Long): Unit = {}
    }
    spark.sparkContext.cleaner.get.attachListener(cleanerListener)

    uncacheTable("t1")
    uncacheTable("t2")

    System.gc()

    eventually(timeout(10.seconds)) {
      assert(toBeCleanedAccIds.synchronized { toBeCleanedAccIds.isEmpty },
        "batchStats accumulators should be cleared after GC when uncacheTable")
    }

    assert(AccumulatorContext.get(accId1).isEmpty)
    assert(AccumulatorContext.get(accId2).isEmpty)
  }

  test("SPARK-10327 Cache Table is not working while subquery has alias in its project list") {
    sparkContext.parallelize((1, 1) :: (2, 2) :: Nil)
      .toDF("key", "value").selectExpr("key", "value", "key+1").createOrReplaceTempView("abc")
    spark.catalog.cacheTable("abc")

    val sparkPlan = sql(
      """select a.key, b.key, c.key from
        |abc a join abc b on a.key=b.key
        |join abc c on a.key=c.key""".stripMargin).queryExecution.sparkPlan

    assert(sparkPlan.collect { case e: InMemoryTableScanExec => e }.size === 3)
    assert(sparkPlan.collect { case e: RDDScanExec => e }.size === 0)
  }

  /**
   * Verifies that the plan for `df` contains `expected` number of Exchange operators.
   */
  private def verifyNumExchanges(df: DataFrame, expected: Int): Unit = {
    assert(
      df.queryExecution.executedPlan.collect { case e: ShuffleExchangeExec => e }.size == expected)
  }

  test("A cached table preserves the partitioning and ordering of its cached SparkPlan") {
    val table3x = testData.union(testData).union(testData)
    table3x.createOrReplaceTempView("testData3x")

    sql("SELECT key, value FROM testData3x ORDER BY key").createOrReplaceTempView("orderedTable")
    spark.catalog.cacheTable("orderedTable")
    assertCached(spark.table("orderedTable"))
    // Should not have an exchange as the query is already sorted on the group by key.
    verifyNumExchanges(sql("SELECT key, count(*) FROM orderedTable GROUP BY key"), 0)
    checkAnswer(
      sql("SELECT key, count(*) FROM orderedTable GROUP BY key ORDER BY key"),
      sql("SELECT key, count(*) FROM testData3x GROUP BY key ORDER BY key").collect())
    uncacheTable("orderedTable")
    spark.catalog.dropTempView("orderedTable")

    // Set up two tables distributed in the same way. Try this with the data distributed into
    // different number of partitions.
    for (numPartitions <- 1 until 10 by 4) {
      withTempView("t1", "t2") {
        testData.repartition(numPartitions, $"key").createOrReplaceTempView("t1")
        testData2.repartition(numPartitions, $"a").createOrReplaceTempView("t2")
        spark.catalog.cacheTable("t1")
        spark.catalog.cacheTable("t2")

        // Joining them should result in no exchanges.
        verifyNumExchanges(sql("SELECT * FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a"), 0)
        checkAnswer(sql("SELECT * FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a"),
          sql("SELECT * FROM testData t1 JOIN testData2 t2 ON t1.key = t2.a"))

        // Grouping on the partition key should result in no exchanges
        verifyNumExchanges(sql("SELECT count(*) FROM t1 GROUP BY key"), 0)
        checkAnswer(sql("SELECT count(*) FROM t1 GROUP BY key"),
          sql("SELECT count(*) FROM testData GROUP BY key"))

        uncacheTable("t1")
        uncacheTable("t2")
      }
    }

    // Distribute the tables into non-matching number of partitions. Need to shuffle one side.
    withTempView("t1", "t2") {
      testData.repartition(6, $"key").createOrReplaceTempView("t1")
      testData2.repartition(3, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      assert(query.queryExecution.executedPlan.outputPartitioning.numPartitions === 6)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    // One side of join is not partitioned in the desired way. Need to shuffle one side.
    withTempView("t1", "t2") {
      testData.repartition(6, $"value").createOrReplaceTempView("t1")
      testData2.repartition(6, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      assert(query.queryExecution.executedPlan.outputPartitioning.numPartitions === 6)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    withTempView("t1", "t2") {
      testData.repartition(6, $"value").createOrReplaceTempView("t1")
      testData2.repartition(12, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      assert(query.queryExecution.executedPlan.outputPartitioning.numPartitions === 12)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    // One side of join is not partitioned in the desired way. Since the number of partitions of
    // the side that has already partitioned is smaller than the side that is not partitioned,
    // we shuffle both side.
    withTempView("t1", "t2") {
      testData.repartition(6, $"value").createOrReplaceTempView("t1")
      testData2.repartition(3, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 2)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    // repartition's column ordering is different from group by column ordering.
    // But they use the same set of columns.
    withTempView("t1") {
      testData.repartition(6, $"value", $"key").createOrReplaceTempView("t1")
      spark.catalog.cacheTable("t1")

      val query = sql("SELECT value, key from t1 group by key, value")
      verifyNumExchanges(query, 0)
      checkAnswer(
        query,
        testData.distinct().select($"value", $"key"))
      uncacheTable("t1")
    }

    // repartition's column ordering is different from join condition's column ordering.
    // We will still shuffle because hashcodes of a row depend on the column ordering.
    // If we do not shuffle, we may actually partition two tables in totally two different way.
    // See PartitioningSuite for more details.
    withTempView("t1", "t2") {
      val df1 = testData
      df1.repartition(6, $"value", $"key").createOrReplaceTempView("t1")
      val df2 = testData2.select($"a", $"b".cast("string"))
      df2.repartition(6, $"a", $"b").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query =
        sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a and t1.value = t2.b")
      verifyNumExchanges(query, 1)
      assert(query.queryExecution.executedPlan.outputPartitioning.numPartitions === 6)
      checkAnswer(
        query,
        df1.join(df2, $"key" === $"a" && $"value" === $"b").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }
  }

  test("SPARK-15870 DataFrame can't execute after uncacheTable") {
    val selectStar = sql("SELECT * FROM testData WHERE key = 1")
    selectStar.createOrReplaceTempView("selectStar")

    spark.catalog.cacheTable("selectStar")
    checkAnswer(
      selectStar,
      Seq(Row(1, "1")))

    uncacheTable("selectStar")
    checkAnswer(
      selectStar,
      Seq(Row(1, "1")))
  }

  test("SPARK-15915 Logical plans should use canonicalized plan when override sameResult") {
    val localRelation = Seq(1, 2, 3).toDF()
    localRelation.createOrReplaceTempView("localRelation")

    spark.catalog.cacheTable("localRelation")
    assert(getNumInMemoryRelations(localRelation) == 1)
  }

  test("SPARK-19093 Caching in side subquery") {
    withTempView("t1") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      spark.catalog.cacheTable("t1")
      val ds =
        sql(
          """
            |SELECT * FROM t1
            |WHERE
            |NOT EXISTS (SELECT * FROM t1)
          """.stripMargin)
      assert(getNumInMemoryRelations(ds) == 2)
    }
  }

  test("SPARK-19093 scalar and nested predicate query") {
    withTempView("t1", "t2", "t3", "t4") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      Seq(2).toDF("c1").createOrReplaceTempView("t2")
      Seq(1).toDF("c1").createOrReplaceTempView("t3")
      Seq(1).toDF("c1").createOrReplaceTempView("t4")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")
      spark.catalog.cacheTable("t3")
      spark.catalog.cacheTable("t4")

      // Nested predicate subquery
      val ds =
        sql(
        """
          |SELECT * FROM t1
          |WHERE
          |c1 IN (SELECT c1 FROM t2 WHERE c1 IN (SELECT c1 FROM t3 WHERE c1 = 1))
        """.stripMargin)
      assert(getNumInMemoryRelations(ds) == 3)

      // Scalar subquery and predicate subquery
      val ds2 =
        sql(
          """
            |SELECT * FROM (SELECT c1, max(c1) FROM t1 GROUP BY c1)
            |WHERE
            |c1 = (SELECT max(c1) FROM t2 GROUP BY c1)
            |OR
            |EXISTS (SELECT c1 FROM t3)
            |OR
            |c1 IN (SELECT c1 FROM t4)
          """.stripMargin)
      assert(getNumInMemoryRelations(ds2) == 4)
    }
  }

  test("SPARK-19765: UNCACHE TABLE should un-cache all cached plans that refer to this table") {
    withTable("t") {
      withTempPath { path =>
        Seq(1 -> "a").toDF("i", "j").write.parquet(path.getCanonicalPath)
        sql(s"CREATE TABLE t USING parquet LOCATION '${path.toURI}'")
        spark.catalog.cacheTable("t")
        spark.table("t").select($"i").cache()
        checkAnswer(spark.table("t").select($"i"), Row(1))
        assertCached(spark.table("t").select($"i"))

        Utils.deleteRecursively(path)
        spark.sessionState.catalog.refreshTable(TableIdentifier("t"))
        uncacheTable("t")
        assert(spark.table("t").select($"i").count() == 0)
        assert(getNumInMemoryRelations(spark.table("t").select($"i")) == 0)
      }
    }
  }

  test("refreshByPath should refresh all cached plans with the specified path") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath()

      spark.range(10).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).cache()
      spark.read.parquet(path).filter($"id" > 4).cache()
      assert(spark.read.parquet(path).filter($"id" > 4).count() == 5)

      spark.range(20).write.mode("overwrite").parquet(path)
      spark.catalog.refreshByPath(path)
      assert(spark.read.parquet(path).count() == 20)
      assert(spark.read.parquet(path).filter($"id" > 4).count() == 15)
    }
  }

  test("SPARK-19993 simple subquery caching") {
    withTempView("t1", "t2") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      Seq(2).toDF("c1").createOrReplaceTempView("t2")

      val sql1 =
        """
          |SELECT * FROM t1
          |WHERE
          |NOT EXISTS (SELECT * FROM t2)
        """.stripMargin
      sql(sql1).cache()

      val cachedDs = sql(sql1)
      assert(getNumInMemoryRelations(cachedDs) == 1)

      // Additional predicate in the subquery plan should cause a cache miss
      val cachedMissDs =
      sql(
        """
          |SELECT * FROM t1
          |WHERE
          |NOT EXISTS (SELECT * FROM t2 where c1 = 0)
        """.stripMargin)
      assert(getNumInMemoryRelations(cachedMissDs) == 0)
    }
  }

  test("SPARK-19993 subquery caching with correlated predicates") {
    withTempView("t1", "t2") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      Seq(1).toDF("c1").createOrReplaceTempView("t2")

      // Simple correlated predicate in subquery
      val sqlText =
        """
          |SELECT * FROM t1
          |WHERE
          |t1.c1 in (SELECT t2.c1 FROM t2 where t1.c1 = t2.c1)
        """.stripMargin
      sql(sqlText).cache()

      val cachedDs = sql(sqlText)
      assert(getNumInMemoryRelations(cachedDs) == 1)
    }
  }

  test("SPARK-19993 subquery with cached underlying relation") {
    withTempView("t1") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      spark.catalog.cacheTable("t1")

      // underlying table t1 is cached as well as the query that refers to it.
      val sqlText =
        """
          |SELECT * FROM t1
          |WHERE
          |NOT EXISTS (SELECT * FROM t1)
        """.stripMargin
      val ds = sql(sqlText)
      assert(getNumInMemoryRelations(ds) == 2)

      val cachedDs = sql(sqlText).cache()
      assert(getNumInMemoryTablesRecursively(cachedDs.queryExecution.sparkPlan) == 3)
    }
  }

  test("SPARK-19993 nested subquery caching and scalar + predicate subqueris") {
    withTempView("t1", "t2", "t3", "t4") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      Seq(2).toDF("c1").createOrReplaceTempView("t2")
      Seq(1).toDF("c1").createOrReplaceTempView("t3")
      Seq(1).toDF("c1").createOrReplaceTempView("t4")

      // Nested predicate subquery
      val sql1 =
        """
          |SELECT * FROM t1
          |WHERE
          |c1 IN (SELECT c1 FROM t2 WHERE c1 IN (SELECT c1 FROM t3 WHERE c1 = 1))
        """.stripMargin
      sql(sql1).cache()

      val cachedDs = sql(sql1)
      assert(getNumInMemoryRelations(cachedDs) == 1)

      // Scalar subquery and predicate subquery
      val sql2 =
        """
          |SELECT * FROM (SELECT c1, max(c1) FROM t1 GROUP BY c1)
          |WHERE
          |c1 = (SELECT max(c1) FROM t2 GROUP BY c1)
          |OR
          |EXISTS (SELECT c1 FROM t3)
          |OR
          |c1 IN (SELECT c1 FROM t4)
        """.stripMargin
      sql(sql2).cache()

      val cachedDs2 = sql(sql2)
      assert(getNumInMemoryRelations(cachedDs2) == 1)
    }
  }

  test("SPARK-23312: vectorized cache reader can be disabled") {
    Seq(true, false).foreach { vectorized =>
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
        val df = spark.range(10).cache()
        df.queryExecution.executedPlan.foreach {
          case i: InMemoryTableScanExec =>
            assert(i.supportsBatch == vectorized && i.supportCodegen == vectorized)
          case _ =>
        }
      }
    }
  }

  private def checkIfNoJobTriggered[T](f: => T): T = {
    var numJobTrigered = 0
    val jobListener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        numJobTrigered += 1
      }
    }
    sparkContext.addSparkListener(jobListener)
    try {
      val result = f
      sparkContext.listenerBus.waitUntilEmpty(10000L)
      assert(numJobTrigered === 0)
      result
    } finally {
      sparkContext.removeSparkListener(jobListener)
    }
  }

  test("SPARK-23880 table cache should be lazy and don't trigger any jobs") {
    val cachedData = checkIfNoJobTriggered {
      spark.range(1002).filter('id > 1000).orderBy('id.desc).cache()
    }
    assert(cachedData.collect === Seq(1001))
  }

  test("SPARK-24596 Non-cascading Cache Invalidation - uncache temporary view") {
    withTempView("t1", "t2") {
      sql("CACHE TABLE t1 AS SELECT * FROM testData WHERE key > 1")
      sql("CACHE TABLE t2 as SELECT * FROM t1 WHERE value > 1")

      assert(spark.catalog.isCached("t1"))
      assert(spark.catalog.isCached("t2"))
      sql("UNCACHE TABLE t1")
      assert(!spark.catalog.isCached("t1"))
      assert(spark.catalog.isCached("t2"))
    }
  }

  test("SPARK-24596 Non-cascading Cache Invalidation - drop temporary view") {
    withTempView("t1", "t2") {
      sql("CACHE TABLE t1 AS SELECT * FROM testData WHERE key > 1")
      sql("CACHE TABLE t2 as SELECT * FROM t1 WHERE value > 1")

      assert(spark.catalog.isCached("t1"))
      assert(spark.catalog.isCached("t2"))
      sql("DROP VIEW t1")
      assert(spark.catalog.isCached("t2"))
    }
  }

  test("SPARK-24596 Non-cascading Cache Invalidation - drop persistent view") {
    withTable("t") {
      spark.range(1, 10).toDF("key").withColumn("value", 'key * 2)
        .write.format("json").saveAsTable("t")
      withView("t1") {
        withTempView("t2") {
          sql("CREATE VIEW t1 AS SELECT * FROM t WHERE key > 1")

          sql("CACHE TABLE t1")
          sql("CACHE TABLE t2 AS SELECT * FROM t1 WHERE value > 1")

          assert(spark.catalog.isCached("t1"))
          assert(spark.catalog.isCached("t2"))
          sql("DROP VIEW t1")
          assert(!spark.catalog.isCached("t2"))
        }
      }
    }
  }

  test("SPARK-24596 Non-cascading Cache Invalidation - uncache table") {
    withTable("t") {
      spark.range(1, 10).toDF("key").withColumn("value", 'key * 2)
        .write.format("json").saveAsTable("t")
      withTempView("t1", "t2") {
        sql("CACHE TABLE t")
        sql("CACHE TABLE t1 AS SELECT * FROM t WHERE key > 1")
        sql("CACHE TABLE t2 AS SELECT * FROM t1 WHERE value > 1")

        assert(spark.catalog.isCached("t"))
        assert(spark.catalog.isCached("t1"))
        assert(spark.catalog.isCached("t2"))
        sql("UNCACHE TABLE t")
        assert(!spark.catalog.isCached("t"))
        assert(!spark.catalog.isCached("t1"))
        assert(!spark.catalog.isCached("t2"))
      }
    }
  }

  test("Cache should respect the hint") {
    def testHint(df: Dataset[_], expectedHint: JoinStrategyHint): Unit = {
      val df2 = spark.range(2000).cache()
      df2.count()

      def checkHintExists(): Unit = {
        // Test the broadcast hint.
        val joinPlan = df.join(df2, "id").queryExecution.optimizedPlan
        val joinHints = joinPlan.collect {
          case Join(_, _, _, _, hint) => hint
        }
        assert(joinHints.size == 1)
        assert(joinHints(0).leftHint.get.strategy.contains(expectedHint))
        assert(joinHints(0).rightHint.isEmpty)
      }

      // Make sure the hint does exist when `df` is not cached.
      checkHintExists()

      df.cache()
      try {
        df.count()
        // Make sure the hint still exists when `df` is cached.
        checkHintExists()
      } finally {
        // Clean-up
        df.unpersist()
      }
    }

    // The hint is the root node
    testHint(broadcast(spark.range(1000)), BROADCAST)
    // The hint is under subquery alias
    testHint(broadcast(spark.range(1000)).as("df"), BROADCAST)
    // The hint is under filter
    testHint(broadcast(spark.range(1000)).filter($"id" > 100), BROADCAST)
    // If there are 2 adjacent hints, the top one takes effect.
    testHint(
      spark.range(1000)
        .hint("SHUFFLE_MERGE")
        .hint("SHUFFLE_HASH")
        .as("df"),
      SHUFFLE_HASH)
  }

  test("analyzes column statistics in cached query") {
    def query(): DataFrame = {
      spark.range(100)
        .selectExpr("id % 3 AS c0", "id % 5 AS c1", "2 AS c2")
        .groupBy("c0")
        .agg(avg("c1").as("v1"), sum("c2").as("v2"))
    }
    // First, checks if there is no column statistic in cached query
    val queryStats1 = query().cache.queryExecution.optimizedPlan.stats.attributeStats
    assert(queryStats1.map(_._1.name).isEmpty)

    val cacheManager = spark.sharedState.cacheManager
    val cachedData = cacheManager.lookupCachedData(query().logicalPlan)
    assert(cachedData.isDefined)
    val queryAttrs = cachedData.get.plan.output
    assert(queryAttrs.size === 3)
    val (c0, v1, v2) = (queryAttrs(0), queryAttrs(1), queryAttrs(2))

    // Analyzes one column in the query output
    cacheManager.analyzeColumnCacheQuery(spark, cachedData.get, v1 :: Nil)
    val queryStats2 = query().queryExecution.optimizedPlan.stats.attributeStats
    assert(queryStats2.map(_._1.name).toSet === Set("v1"))

    // Analyzes two more columns
    cacheManager.analyzeColumnCacheQuery(spark, cachedData.get, c0 :: v2 :: Nil)
    val queryStats3 = query().queryExecution.optimizedPlan.stats.attributeStats
    assert(queryStats3.map(_._1.name).toSet === Set("c0", "v1", "v2"))
  }

  test("SPARK-27248 refreshTable should recreate cache with same cache name and storage level") {
    // This section tests when a table is cached with its qualified name but it is refreshed with
    // its unqualified name.
    withTempDatabase { db =>
      withTempPath { path =>
        withTable(s"$db.cachedTable") {
          // Create table 'cachedTable' in temp db for testing purpose.
          spark.catalog.createTable(
            s"$db.cachedTable",
            "PARQUET",
            StructType(Array(StructField("key", StringType))),
            Map("LOCATION" -> path.toURI.toString))

          withCache(s"$db.cachedTable") {
            // Cache the table 'cachedTable' in temp db with qualified table name with storage level
            // MEMORY_ONLY, and then check whether the table is cached with expected name and
            // storage level.
            spark.catalog.cacheTable(s"$db.cachedTable", MEMORY_ONLY)
            assertCached(spark.table(s"$db.cachedTable"), s"$db.cachedTable", MEMORY_ONLY)
            assert(spark.catalog.isCached(s"$db.cachedTable"),
              s"Table '$db.cachedTable' should be cached.")

            // Refresh the table 'cachedTable' in temp db with qualified table name, and then check
            // whether the table is still cached with the same name and storage level.
            // Without bug fix 'SPARK-27248', the recreated cache storage level will be default
            // storage level 'MEMORY_AND_DISK', instead of 'MEMORY_ONLY'.
            spark.catalog.refreshTable(s"$db.cachedTable")
            assertCached(spark.table(s"$db.cachedTable"), s"$db.cachedTable", MEMORY_ONLY)
            assert(spark.catalog.isCached(s"$db.cachedTable"),
              s"Table '$db.cachedTable' should be cached after refreshing with its qualified name.")

            // Change the active database to the temp db and refresh the table with unqualified
            // table name, and then check whether the table is still cached with the same name and
            // storage level.
            // Without bug fix 'SPARK-27248', the recreated cache name will be changed to
            // 'cachedTable', instead of '$db.cachedTable'
            activateDatabase(db) {
              spark.catalog.refreshTable("cachedTable")
              assertCached(spark.table("cachedTable"), s"$db.cachedTable", MEMORY_ONLY)
              assert(spark.catalog.isCached("cachedTable"),
                s"Table '$db.cachedTable' should be cached after refreshing with its " +
                  "unqualified name.")
            }
          }
        }
      }

      // This section tests when a table is cached with its unqualified name but it is refreshed
      // with its qualified name.
      withTempPath { path =>
        withTable("cachedTable") {
          // Create table 'cachedTable' in default db for testing purpose.
          spark.catalog.createTable(
            "cachedTable",
            "PARQUET",
            StructType(Array(StructField("key", StringType))),
            Map("LOCATION" -> path.toURI.toString))
          withCache("cachedTable") {
            // Cache the table 'cachedTable' in default db without qualified table name with storage
            // level 'MEMORY_AND_DISK2', and then check whether the table is cached with expected
            // name and storage level.
            spark.catalog.cacheTable("cachedTable", MEMORY_AND_DISK_2)
            assertCached(spark.table("cachedTable"), "cachedTable", MEMORY_AND_DISK_2)
            assert(spark.catalog.isCached("cachedTable"),
              "Table 'cachedTable' should be cached.")

            // Refresh the table 'cachedTable' in default db with unqualified table name, and then
            // check whether the table is still cached with the same name and storage level.
            // Without bug fix 'SPARK-27248', the recreated cache storage level will be default
            // storage level 'MEMORY_AND_DISK', instead of 'MEMORY_AND_DISK2'.
            spark.catalog.refreshTable("cachedTable")
            assertCached(spark.table("cachedTable"), "cachedTable", MEMORY_AND_DISK_2)
            assert(spark.catalog.isCached("cachedTable"),
              "Table 'cachedTable' should be cached after refreshing with its unqualified name.")

            // Change the active database to the temp db and refresh the table with qualified
            // table name, and then check whether the table is still cached with the same name and
            // storage level.
            // Without bug fix 'SPARK-27248', the recreated cache name will be changed to
            // 'default.cachedTable', instead of 'cachedTable'
            activateDatabase(db) {
              spark.catalog.refreshTable("default.cachedTable")
              assertCached(spark.table("default.cachedTable"), "cachedTable", MEMORY_AND_DISK_2)
              assert(spark.catalog.isCached("default.cachedTable"),
                "Table 'cachedTable' should be cached after refreshing with its qualified name.")
            }
          }
        }
      }
    }
  }
}
