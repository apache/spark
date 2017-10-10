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

import org.apache.spark.sql.test.SharedSQLContext

class KeyValueGroupedDatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("Check KeyValueGroupedDataset toString: Single data") {
    val kvDataset = (1 to 3).toDF("id").as[SingleData].groupByKey(identity)
    val expected = "KeyValueGroupedDataset: [key: [id: int], value: [id: int]]"
    val actual = kvDataset.toString
    checkString(expected, actual)
  }

  test("Check KeyValueGroupedDataset toString: Unnamed KV-pair") {
    val kvDataset = (1 to 3).map(x => (x, x.toString))
      .toDF("id", "val1").as[DoubleData].groupByKey(x => (x.id, x.val1))
    val expected = "KeyValueGroupedDataset:" +
      " [key: [_1: int, _2: string]," +
      " value: [id: int, val1: string]]"
    val actual = kvDataset.toString
    checkString(expected, actual)
  }

  test("Check KeyValueGroupedDataset toString: Named KV-pair") {
    val kvDataset = (1 to 3).map( x => (x, x.toString))
      .toDF("id", "val1").as[DoubleData].groupByKey(x => DoubleData(x.id, x.val1))
    val expected = "KeyValueGroupedDataset:" +
      " [key: [id: int, val1: string]," +
      " value: [id: int, val1: string]]"
    val actual = kvDataset.toString
    checkString(expected, actual)
  }

  test("Check KeyValueGroupedDataset toString: over length schema ") {
    val kvDataset = (1 to 3).map( x => (x, x.toString, x.toLong))
      .toDF("id", "val1", "val2").as[TripleData].groupByKey(identity)
    val expected = "KeyValueGroupedDataset:" +
      " [key: [id: int, val1: string ... 1 more field(s)]," +
      " value: [id: int, val1: string ... 1 more field(s)]]"
    val actual = kvDataset.toString
    checkString(expected, actual)
  }
}
