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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData._


class PartitionBatchPruningSuite extends SharedSparkSession {

  import testImplicits._

  private lazy val originalColumnBatchSize = spark.conf.get(SQLConf.COLUMN_BATCH_SIZE)
  private lazy val originalInMemoryPartitionPruning =
    spark.conf.get(SQLConf.IN_MEMORY_PARTITION_PRUNING)
  private val testArrayData = (1 to 100).map { key =>
    Tuple1(Array.fill(key)(key))
  }
  private val testBinaryData = (1 to 100).map { key =>
    Tuple1(Array.fill(key)(key.toByte))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Make a table with 5 partitions, 2 batches per partition, 10 elements per batch
    spark.conf.set(SQLConf.COLUMN_BATCH_SIZE.key, 10)
    // Enable in-memory partition pruning
    spark.conf.set(SQLConf.IN_MEMORY_PARTITION_PRUNING.key, true)
    // Enable in-memory table scan accumulators
    spark.conf.set(SQLConf.IN_MEMORY_TABLE_SCAN_STATISTICS_ENABLED.key, "true")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.conf.set(SQLConf.COLUMN_BATCH_SIZE.key, originalColumnBatchSize)
      spark.conf.set(SQLConf.IN_MEMORY_PARTITION_PRUNING.key, originalInMemoryPartitionPruning)
    } finally {
      super.afterAll()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // This creates accumulators, which get cleaned up after every single test,
    // so we need to do this before every test.
    val pruningData = sparkContext.makeRDD((1 to 100).map { key =>
      val string = if (((key - 1) / 10) % 2 == 0) null else key.toString
      TestData(key, string)
    }, 5).toDF()
    pruningData.createOrReplaceTempView("pruningData")
    spark.catalog.cacheTable("pruningData")

    val pruningStringData = sparkContext.makeRDD((100 to 200).map { key =>
      StringData(key.toString)
    }, 5).toDF()
    pruningStringData.createOrReplaceTempView("pruningStringData")
    spark.catalog.cacheTable("pruningStringData")

    val pruningArrayData = sparkContext.makeRDD(testArrayData, 5).toDF()
    pruningArrayData.createOrReplaceTempView("pruningArrayData")
    spark.catalog.cacheTable("pruningArrayData")

    val pruningBinaryData = sparkContext.makeRDD(testBinaryData, 5).toDF()
    pruningBinaryData.createOrReplaceTempView("pruningBinaryData")
    spark.catalog.cacheTable("pruningBinaryData")
  }

  override protected def afterEach(): Unit = {
    try {
      spark.catalog.uncacheTable("pruningData")
      spark.catalog.uncacheTable("pruningStringData")
      spark.catalog.uncacheTable("pruningArrayData")
      spark.catalog.uncacheTable("pruningBinaryData")
    } finally {
      super.afterEach()
    }
  }

  // Comparisons
  checkBatchPruning("SELECT key FROM pruningData WHERE key = 1", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE 1 = key", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE key <=> 1", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE 1 <=> key", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE key < 12", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE key <= 11", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE key > 88", 1, 2)(89 to 100)
  checkBatchPruning("SELECT key FROM pruningData WHERE key >= 89", 1, 2)(89 to 100)
  checkBatchPruning("SELECT key FROM pruningData WHERE 12 > key", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE 11 >= key", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE 88 < key", 1, 2)(89 to 100)
  checkBatchPruning("SELECT key FROM pruningData WHERE 89 <= key", 1, 2)(89 to 100)
  // Do not filter on array type
  checkBatchPruning("SELECT _1 FROM pruningArrayData WHERE _1 = array(1)", 5, 10)(Seq(Array(1)))
  checkBatchPruning("SELECT _1 FROM pruningArrayData WHERE _1 <= array(1)", 5, 10)(Seq(Array(1)))
  checkBatchPruning("SELECT _1 FROM pruningArrayData WHERE _1 >= array(1)", 5, 10)(
    testArrayData.map(_._1))
  // Do not filter on binary type
  checkBatchPruning(
    "SELECT _1 FROM pruningBinaryData WHERE _1 == binary(chr(1))", 5, 10)(Seq(Array(1.toByte)))

  // IS NULL
  checkBatchPruning("SELECT key FROM pruningData WHERE value IS NULL", 5, 5) {
    (1 to 10) ++ (21 to 30) ++ (41 to 50) ++ (61 to 70) ++ (81 to 90)
  }

  // IS NOT NULL
  checkBatchPruning("SELECT key FROM pruningData WHERE value IS NOT NULL", 5, 5) {
    (11 to 20) ++ (31 to 40) ++ (51 to 60) ++ (71 to 80) ++ (91 to 100)
  }

  // Conjunction and disjunction
  checkBatchPruning("SELECT key FROM pruningData WHERE key > 8 AND key <= 21", 2, 3)(9 to 21)
  checkBatchPruning("SELECT key FROM pruningData WHERE key < 2 OR key > 99", 2, 2)(Seq(1, 100))
  checkBatchPruning("SELECT key FROM pruningData WHERE key < 12 AND key IS NOT NULL", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE key < 2 OR (key > 78 AND key < 92)", 3, 4) {
    Seq(1) ++ (79 to 91)
  }
  checkBatchPruning("SELECT key FROM pruningData WHERE NOT (key < 88)", 1, 2) {
    // Although the `NOT` operator isn't supported directly, the optimizer can transform
    // `NOT (a < b)` to `b >= a`
    88 to 100
  }

  // Support `IN` predicate
  checkBatchPruning("SELECT key FROM pruningData WHERE key IN (1)", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE key IN (1, 2)", 1, 1)(Seq(1, 2))
  checkBatchPruning("SELECT key FROM pruningData WHERE key IN (1, 11)", 1, 2)(Seq(1, 11))
  checkBatchPruning("SELECT key FROM pruningData WHERE key IN (1, 21, 41, 61, 81)", 5, 5)(
    Seq(1, 21, 41, 61, 81))
  checkBatchPruning("SELECT CAST(s AS INT) FROM pruningStringData WHERE s = '100'", 1, 1)(Seq(100))
  checkBatchPruning("SELECT CAST(s AS INT) FROM pruningStringData WHERE s < '102'", 1, 1)(
    Seq(100, 101))
  checkBatchPruning(
    "SELECT CAST(s AS INT) FROM pruningStringData WHERE s IN ('99', '150', '201')", 1, 1)(
      Seq(150))
  // Do not filter on array type
  checkBatchPruning("SELECT _1 FROM pruningArrayData WHERE _1 IN (array(1), array(2, 2))", 5, 10)(
    Seq(Array(1), Array(2, 2)))

  // With unsupported `InSet` predicate
  {
    val seq = (1 to 30).mkString(", ")
    checkBatchPruning(s"SELECT key FROM pruningData WHERE key IN ($seq)", 5, 10)(1 to 30)
    checkBatchPruning(s"SELECT key FROM pruningData WHERE NOT (key IN ($seq))", 5, 10)(31 to 100)
    checkBatchPruning(s"SELECT key FROM pruningData WHERE NOT (key IN ($seq)) AND key > 88", 1, 2) {
      89 to 100
    }
  }

  // Support `StartsWith` predicate
  checkBatchPruning("SELECT CAST(s AS INT) FROM pruningStringData WHERE s like '18%'", 1, 1)(
    180 to 189
  )
  checkBatchPruning("SELECT CAST(s AS INT) FROM pruningStringData WHERE s like '%'", 5, 11)(
    100 to 200
  )
  checkBatchPruning("SELECT CAST(s AS INT) FROM pruningStringData WHERE '18%' like s", 5, 11)(Seq())

  // With disable IN_MEMORY_PARTITION_PRUNING option
  test("disable IN_MEMORY_PARTITION_PRUNING") {
    spark.conf.set(SQLConf.IN_MEMORY_PARTITION_PRUNING.key, false)

    val df = sql("SELECT key FROM pruningData WHERE key = 1")
    val result = df.collect().map(_(0)).toArray
    assert(result.length === 1)

    val (readPartitions, readBatches) = df.queryExecution.executedPlan.collect {
        case in: InMemoryTableScanExec => (in.readPartitions.value, in.readBatches.value)
      }.head
    assert(readPartitions === 5)
    assert(readBatches === 10)
  }

  def checkBatchPruning(
      query: String,
      expectedReadPartitions: Int,
      expectedReadBatches: Int)(
      expectedQueryResult: => Seq[Any]): Unit = {

    test(query) {
      val df = sql(query)
      val queryExecution = df.queryExecution

      assertResult(expectedQueryResult.toArray, s"Wrong query result: $queryExecution") {
        df.collect().map(_(0)).toArray
      }

      val (readPartitions, readBatches) = df.queryExecution.executedPlan.collect {
        case in: InMemoryTableScanExec => (in.readPartitions.value, in.readBatches.value)
      }.head

      assert(readBatches === expectedReadBatches, s"Wrong number of read batches: $queryExecution")
      assert(
        readPartitions === expectedReadPartitions,
        s"Wrong number of read partitions: $queryExecution")
    }
  }
}
