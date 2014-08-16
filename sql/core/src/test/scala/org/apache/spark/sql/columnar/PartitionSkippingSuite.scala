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

import org.scalatest.FunSuite

import org.apache.spark.sql._
import org.apache.spark.sql.test.TestSQLContext._

case class IntegerData(i: Int)

class PartitionSkippingSuite extends FunSuite {
  test("In-Memory Columnar Scan Skips Partitions") {
    val rawData = sparkContext.makeRDD(1 to 100, 10).map(IntegerData)
    rawData.registerTempTable("intData")
    cacheTable("intData")

    val query = sql("SELECT * FROM intData WHERE i = 1")
    assert(query.collect().toSeq === Seq(Row(1)))

    val numPartitionsRead = query.queryExecution.executedPlan.collect {
      case in: InMemoryColumnarTableScan => in.readPartitions.value
    }.head

    assert(numPartitionsRead === 1)
  }
}