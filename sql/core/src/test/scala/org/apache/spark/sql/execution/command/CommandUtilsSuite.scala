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

package org.apache.spark.sql.execution.command

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics

class CommandUtilsSuite extends SparkFunSuite {

  test("Check if compareAndGetNewStats returns correct results") {
    val oldStats1 = CatalogStatistics(sizeInBytes = 10, None, rowCount = Some(100))
    val newStats1 = CommandUtils.compareAndGetNewStats(
      Some(oldStats1), SizeInBytesWithDeserFactor(10, None), newRowCount = Some(100))
    assert(newStats1.isEmpty)
    val newStats2 = CommandUtils.compareAndGetNewStats(
      Some(oldStats1), SizeInBytesWithDeserFactor(-1, None), newRowCount = None)
    assert(newStats2.isEmpty)
    val newStats3 = CommandUtils.compareAndGetNewStats(
      Some(oldStats1), SizeInBytesWithDeserFactor(20, None), newRowCount = Some(-1))
    assert(newStats3.isDefined)
    newStats3.foreach { stat =>
      assert(stat.sizeInBytes === 20)
      assert(stat.rowCount.isEmpty)
    }
    val newStats4 = CommandUtils.compareAndGetNewStats(
      Some(oldStats1), SizeInBytesWithDeserFactor(-1, None), newRowCount = Some(200))
    assert(newStats4.isDefined)
    newStats4.foreach { stat =>
      assert(stat.sizeInBytes === 10)
      assert(stat.rowCount.isDefined && stat.rowCount.get === 200)
    }
  }

  test("Check if compareAndGetNewStats can handle large values") {
    // Tests for large values
    val oldStats2 = CatalogStatistics(sizeInBytes = BigInt(Long.MaxValue) * 2, None)
    val newStats5 = CommandUtils.compareAndGetNewStats(
      Some(oldStats2), SizeInBytesWithDeserFactor(BigInt(Long.MaxValue) * 2, None), None)
    assert(newStats5.isEmpty)
  }

  test("compareAndGetNewStats with deserialization factor") {
    val oldStats3 = Some(CatalogStatistics(sizeInBytes = BigInt(1), Some(2), Some(300)))
    val newStats6 = CommandUtils.compareAndGetNewStats(
      oldStats3, SizeInBytesWithDeserFactor(BigInt(1), deserFactor = None), Some(300))
    assert(newStats6.isEmpty,
      "compare must return None here as the old deserFactor should be inherited when its " +
        "calculation disabled at subsequent runs")

    val newStats7 = CommandUtils.compareAndGetNewStats(
      oldStats3, SizeInBytesWithDeserFactor(BigInt(1), deserFactor = Some(2)), Some(300))
    assert(newStats7.isEmpty)

    val newStats8 = CommandUtils.compareAndGetNewStats(
      oldStats3, SizeInBytesWithDeserFactor(BigInt(5), deserFactor = Some(2)), Some(300))
    // the rowCount is None here as its value have not been changed
    assert(newStats8.isDefined &&
      newStats8.get === CatalogStatistics(BigInt(5), deserFactor = Some(2), None),
      "sizeInBytes is changed so a new catalog statistics is needed")

    val newStats9 = CommandUtils.compareAndGetNewStats(
      oldStats3, SizeInBytesWithDeserFactor(BigInt(1), deserFactor = Some(4)), Some(300))
    // the rowCount is None here as its value have not been changed
    assert(newStats9.isDefined &&
      newStats9.get === CatalogStatistics(BigInt(1), deserFactor = Some(4), None),
      "factor is changed so a new catalog statistics is needed")
  }
}
