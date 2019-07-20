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

import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.storage.StorageLevel


class DatasetCacheSuite extends QueryTest with SharedSQLContext with TimeLimits {
  import testImplicits._

  /**
   * Asserts that a cached [[Dataset]] will be built using the given number of other cached results.
   */
  private def assertCacheDependency(df: DataFrame, numOfCachesDependedUpon: Int = 1): Unit = {
    val plan = df.queryExecution.withCachedData
    assert(plan.isInstanceOf[InMemoryRelation])
    val internalPlan = plan.asInstanceOf[InMemoryRelation].cacheBuilder.cachedPlan
    assert(internalPlan.find(_.isInstanceOf[InMemoryTableScanExec]).size == numOfCachesDependedUpon)
  }

  test("get storage level") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    // default storage level
    ds1.persist()
    ds2.cache()
    assert(ds1.storageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(ds2.storageLevel == StorageLevel.MEMORY_AND_DISK)
    // unpersist
    ds1.unpersist(blocking = true)
    assert(ds1.storageLevel == StorageLevel.NONE)
    // non-default storage level
    ds1.persist(StorageLevel.MEMORY_ONLY_2)
    assert(ds1.storageLevel == StorageLevel.MEMORY_ONLY_2)
    // joined Dataset should not be persisted
    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    assert(joined.storageLevel == StorageLevel.NONE)
  }

  test("persist and unpersist") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS().select(expr("_2 + 1").as[Int])
    val cached = ds.cache()
    // count triggers the caching action. It should not throw.
    cached.count()
    // Make sure, the Dataset is indeed cached.
    assertCached(cached)
    // Check result.
    checkDataset(
      cached,
      2, 3, 4)
    // Drop the cache.
    cached.unpersist(blocking = true)
    assert(cached.storageLevel == StorageLevel.NONE, "The Dataset should not be cached.")
  }

  test("persist and then rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    ds1.persist()
    assertCached(ds1)
    ds2.persist()
    assertCached(ds2)

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    checkDataset(joined, ("2", 2))
    assertCached(joined, 2)

    ds1.unpersist(blocking = true)
    assert(ds1.storageLevel == StorageLevel.NONE, "The Dataset ds1 should not be cached.")
    ds2.unpersist(blocking = true)
    assert(ds2.storageLevel == StorageLevel.NONE, "The Dataset ds2 should not be cached.")
  }

  test("persist and then groupBy columns asKey, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(_._1)
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }
    agged.persist()

    checkDataset(
      agged.filter(_._1 == "b"),
      ("b", 3))
    assertCached(agged.filter(_._1 == "b"))

    ds.unpersist(blocking = true)
    assert(ds.storageLevel == StorageLevel.NONE, "The Dataset ds should not be cached.")
    agged.unpersist(blocking = true)
    assert(agged.storageLevel == StorageLevel.NONE, "The Dataset agged should not be cached.")
  }

  test("persist and then withColumn") {
    val df = Seq(("test", 1)).toDF("s", "i")
    val df2 = df.withColumn("newColumn", lit(1))

    df.cache()
    assertCached(df)
    assertCached(df2)

    df.count()
    assertCached(df2)

    df.unpersist(blocking = true)
    assert(df.storageLevel == StorageLevel.NONE)
  }

  test("cache UDF result correctly") {
    val expensiveUDF = udf({x: Int => Thread.sleep(2000); x})
    val df = spark.range(0, 2).toDF("a").repartition(1).withColumn("b", expensiveUDF($"a"))
    val df2 = df.agg(sum(df("b")))

    df.cache()
    df.count()
    assertCached(df2)

    // udf has been evaluated during caching, and thus should not be re-evaluated here
    failAfter(2.seconds) {
      df2.collect()
    }

    df.unpersist(blocking = true)
    assert(df.storageLevel == StorageLevel.NONE)
  }

  test("SPARK-24613 Cache with UDF could not be matched with subsequent dependent caches") {
    val udf1 = udf({x: Int => x + 1})
    val df = spark.range(0, 10).toDF("a").withColumn("b", udf1($"a"))
    val df2 = df.agg(sum(df("b")))

    df.cache()
    df.count()
    df2.cache()

    assertCacheDependency(df2)
  }

  test("SPARK-24596 Non-cascading Cache Invalidation") {
    val df = Seq(("a", 1), ("b", 2)).toDF("s", "i")
    val df2 = df.filter('i > 1)
    val df3 = df.filter('i < 2)

    df2.cache()
    df.cache()
    df.count()
    df3.cache()

    df.unpersist(blocking = true)

    // df un-cached; df2 and df3's cache plan re-compiled
    assert(df.storageLevel == StorageLevel.NONE)
    assertCacheDependency(df2, 0)
    assertCacheDependency(df3, 0)
  }

  test("SPARK-24596 Non-cascading Cache Invalidation - verify cached data reuse") {
    val expensiveUDF = udf({ x: Int => Thread.sleep(5000); x })
    val df = spark.range(0, 5).toDF("a")
    val df1 = df.withColumn("b", expensiveUDF($"a"))
    val df2 = df1.groupBy('a).agg(sum('b))
    val df3 = df.agg(sum('a))

    df1.cache()
    df2.cache()
    df2.collect()
    df3.cache()

    assertCacheDependency(df2)

    df1.unpersist(blocking = true)

    // df1 un-cached; df2's cache plan stays the same
    assert(df1.storageLevel == StorageLevel.NONE)
    assertCacheDependency(df1.groupBy('a).agg(sum('b)))

    val df4 = df1.groupBy('a).agg(sum('b)).agg(sum("sum(b)"))
    assertCached(df4)
    // reuse loaded cache
    failAfter(3.seconds) {
      checkDataset(df4, Row(10))
    }

    val df5 = df.agg(sum('a)).filter($"sum(a)" > 1)
    assertCached(df5)
    // first time use, load cache
    checkDataset(df5, Row(10))
  }

  test("SPARK-26708 Cache data and cached plan should stay consistent") {
    val df = spark.range(0, 5).toDF("a")
    val df1 = df.withColumn("b", 'a + 1)
    val df2 = df.filter('a > 1)

    df.cache()
    // Add df1 to the CacheManager; the buffer is currently empty.
    df1.cache()
    // After calling collect(), df1's buffer has been loaded.
    df1.collect()
    // Add df2 to the CacheManager; the buffer is currently empty.
    df2.cache()

    // Verify that df1 is a InMemoryRelation plan with dependency on another cached plan.
    assertCacheDependency(df1)
    val df1InnerPlan = df1.queryExecution.withCachedData
      .asInstanceOf[InMemoryRelation].cacheBuilder.cachedPlan
    // Verify that df2 is a InMemoryRelation plan with dependency on another cached plan.
    assertCacheDependency(df2)

    df.unpersist(blocking = true)

    // Verify that df1's cache has stayed the same, since df1's cache already has data
    // before df.unpersist().
    val df1Limit = df1.limit(2)
    val df1LimitInnerPlan = df1Limit.queryExecution.withCachedData.collectFirst {
      case i: InMemoryRelation => i.cacheBuilder.cachedPlan
    }
    assert(df1LimitInnerPlan.isDefined && df1LimitInnerPlan.get == df1InnerPlan)

    // Verify that df2's cache has been re-cached, with a new physical plan rid of dependency
    // on df, since df2's cache had not been loaded before df.unpersist().
    val df2Limit = df2.limit(2)
    val df2LimitInnerPlan = df2Limit.queryExecution.withCachedData.collectFirst {
      case i: InMemoryRelation => i.cacheBuilder.cachedPlan
    }
    assert(df2LimitInnerPlan.isDefined &&
      df2LimitInnerPlan.get.find(_.isInstanceOf[InMemoryTableScanExec]).isEmpty)
  }
}
