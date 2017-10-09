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

package org.apache.spark.sql.test

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.sideBySide

case class TestData1(id: Int)
case class TestData2(id: Int, val1: String)
case class TestData3(id: Int, val1: String, val2: Long)

class KeyValueGroupedDatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def checkString(expected: String, actual: String): Unit = {
    if (expected != actual) {
      fail(
        "KeyValueGroupedDataset.toString() gives wrong result:\n\n" + sideBySide(
          "== Expected ==\n" + expected,
          "== Actual ==\n" + actual
        ).mkString("\n")
      )
    }
  }

  test("Check KeyValueGroupedDataset toString: Sigle data") {
    val kvDataset = (1 to 3).toDF("id").as[TestData1].groupByKey(identity)
    val expected = "KeyValueGroupedDataset: [key: [id: int], value: [id: int]]"
    val actual = kvDataset.toString

    checkString(expected, actual)
  }

  test("Check KeyValueGroupedDataset toString: Unnamed KV-pair") {
    val kvDataset = (1 to 3).map(x => (x, x.toString))
      .toDF("id", "val1").as[TestData2].groupByKey(x => (x.id, x.val1))
    val expected = "KeyValueGroupedDataset:" +
      " [key: [_1: int, _2: string]," +
      " value: [id: int, val1: string]]"

    val actual = kvDataset.toString

    checkString(expected, actual)

  }

  test("Check KeyValueGroupedDataset toString: Named KV-pair") {
    val kvDataset = (1 to 3).map( x => (x, x.toString))
      .toDF("id", "val1").as[TestData2].groupByKey(x => TestData2(x.id, x.val1))
    val expected = "KeyValueGroupedDataset:" +
      " [key: [id: int, val1: string]," +
      " value: [id: int, val1: string]]"

    val actual = kvDataset.toString
    checkString(expected, actual)
  }

  test("Check KeyValueGroupedDataset toString: over length schema ") {
    val kvDataset = (1 to 3).map( x => (x, x.toString, x.toLong))
      .toDF("id", "val1", "val2").as[TestData3].groupByKey(identity)
    val expected = "KeyValueGroupedDataset:" +
      " [key: [id: int, val1: string ... 1 more field(s)]," +
      " value: [id: int, val1: string ... 1 more field(s)]]"

    val actual = kvDataset.toString
    checkString(expected, actual)
  }
}
