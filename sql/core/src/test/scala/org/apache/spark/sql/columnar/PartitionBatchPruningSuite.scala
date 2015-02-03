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

package org.apache.spark.sql.columnar

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql._
import org.apache.spark.sql.test.TestSQLContext._

class PartitionBatchPruningSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  val originalColumnBatchSize = conf.columnBatchSize
  val originalInMemoryPartitionPruning = conf.inMemoryPartitionPruning

  override protected def beforeAll(): Unit = {
    // Make a table with 5 partitions, 2 batches per partition, 10 elements per batch
    setConf(SQLConf.COLUMN_BATCH_SIZE, "10")

    val pruningData = sparkContext.makeRDD((1 to 100).map { key =>
      val string = if (((key - 1) / 10) % 2 == 0) null else key.toString
      TestData(key, string)
    }, 5)
    pruningData.registerTempTable("pruningData")

    // Enable in-memory partition pruning
    setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, "true")
  }

  override protected def afterAll(): Unit = {
    setConf(SQLConf.COLUMN_BATCH_SIZE, originalColumnBatchSize.toString)
    setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, originalInMemoryPartitionPruning.toString)
  }

  before {
    cacheTable("pruningData")
  }

  after {
    uncacheTable("pruningData")
  }

  // Comparisons
  checkBatchPruning("SELECT key FROM pruningData WHERE key = 1", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE 1 = key", 1, 1)(Seq(1))
  checkBatchPruning("SELECT key FROM pruningData WHERE key < 12", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE key <= 11", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE key > 88", 1, 2)(89 to 100)
  checkBatchPruning("SELECT key FROM pruningData WHERE key >= 89", 1, 2)(89 to 100)
  checkBatchPruning("SELECT key FROM pruningData WHERE 12 > key", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE 11 >= key", 1, 2)(1 to 11)
  checkBatchPruning("SELECT key FROM pruningData WHERE 88 < key", 1, 2)(89 to 100)
  checkBatchPruning("SELECT key FROM pruningData WHERE 89 <= key", 1, 2)(89 to 100)

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

  // With unsupported predicate
  {
    val seq = (1 to 30).mkString(", ")
    checkBatchPruning(s"SELECT key FROM pruningData WHERE NOT (key IN ($seq))", 5, 10)(31 to 100)
    checkBatchPruning(s"SELECT key FROM pruningData WHERE NOT (key IN ($seq)) AND key > 88", 1, 2) {
      89 to 100
    }
  }

  def checkBatchPruning(
      query: String,
      expectedReadPartitions: Int,
      expectedReadBatches: Int)(
      expectedQueryResult: => Seq[Int]): Unit = {

    test(query) {
      val df = sql(query)
      val queryExecution = df.queryExecution

      assertResult(expectedQueryResult.toArray, s"Wrong query result: $queryExecution") {
        df.collect().map(_(0)).toArray
      }

      val (readPartitions, readBatches) = df.queryExecution.executedPlan.collect {
        case in: InMemoryColumnarTableScan => (in.readPartitions.value, in.readBatches.value)
      }.head

      assert(readBatches === expectedReadBatches, s"Wrong number of read batches: $queryExecution")
      assert(
        readPartitions === expectedReadPartitions,
        s"Wrong number of read partitions: $queryExecution")
    }
  }
}
