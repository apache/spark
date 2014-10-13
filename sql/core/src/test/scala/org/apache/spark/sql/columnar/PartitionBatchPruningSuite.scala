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

case class IntegerData(i: Int)

class PartitionBatchPruningSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  val originalColumnBatchSize = columnBatchSize
  val originalInMemoryPartitionPruning = inMemoryPartitionPruning

  override protected def beforeAll(): Unit = {
    // Make a table with 5 partitions, 2 batches per partition, 10 elements per batch
    setConf(SQLConf.COLUMN_BATCH_SIZE, "10")
    val rawData = sparkContext.makeRDD(1 to 100, 5).map(IntegerData)
    rawData.registerTempTable("intData")

    // Enable in-memory partition pruning
    setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, "true")
  }

  override protected def afterAll(): Unit = {
    setConf(SQLConf.COLUMN_BATCH_SIZE, originalColumnBatchSize.toString)
    setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, originalInMemoryPartitionPruning.toString)
  }

  before {
    cacheTable("intData")
  }

  after {
    uncacheTable("intData")
  }

  // Comparisons
  checkBatchPruning("i = 1", Seq(1), 1, 1)
  checkBatchPruning("1 = i", Seq(1), 1, 1)
  checkBatchPruning("i < 12", 1 to 11, 1, 2)
  checkBatchPruning("i <= 11", 1 to 11, 1, 2)
  checkBatchPruning("i > 88", 89 to 100, 1, 2)
  checkBatchPruning("i >= 89", 89 to 100, 1, 2)
  checkBatchPruning("12 > i", 1 to 11, 1, 2)
  checkBatchPruning("11 >= i", 1 to 11, 1, 2)
  checkBatchPruning("88 < i", 89 to 100, 1, 2)
  checkBatchPruning("89 <= i", 89 to 100, 1, 2)

  // Conjunction and disjunction
  checkBatchPruning("i > 8 AND i <= 21", 9 to 21, 2, 3)
  checkBatchPruning("i < 2 OR i > 99", Seq(1, 100), 2, 2)
  checkBatchPruning("i < 2 OR (i > 78 AND i < 92)", Seq(1) ++ (79 to 91), 3, 4)
  checkBatchPruning("NOT (i < 88)", 88 to 100, 1, 2)

  // With unsupported predicate
  checkBatchPruning("i < 12 AND i IS NOT NULL", 1 to 11, 1, 2)
  checkBatchPruning(s"NOT (i in (${(1 to 30).mkString(",")}))", 31 to 100, 5, 10)

  def checkBatchPruning(
      filter: String,
      expectedQueryResult: Seq[Int],
      expectedReadPartitions: Int,
      expectedReadBatches: Int): Unit = {

    test(filter) {
      val query = sql(s"SELECT * FROM intData WHERE $filter")
      assertResult(expectedQueryResult.toArray, "Wrong query result") {
        query.collect().map(_.head).toArray
      }

      val (readPartitions, readBatches) = query.queryExecution.executedPlan.collect {
        case in: InMemoryColumnarTableScan => (in.readPartitions.value, in.readBatches.value)
      }.head

      assert(readBatches === expectedReadBatches, "Wrong number of read batches")
      assert(readPartitions === expectedReadPartitions, "Wrong number of read partitions")
    }
  }
}
