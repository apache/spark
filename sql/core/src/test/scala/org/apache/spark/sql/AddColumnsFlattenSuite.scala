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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession


class AddColumnsFlattenSuite extends QueryTest
  with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("withColumns: check no new project addition for simple columns addition") {
    val testDf = spark.range(1).select($"id" as "a", $"id" as "b")
    val initNodes = testDf.queryExecution.logical.collect {
      case l => l
    }

    val newDf = testDf.withColumns(Seq("newCol1", "newCol2"),
      Seq(col("a") + 1, col("b") + 2))

    val newNodes = newDf.queryExecution.logical.collect {
      case l => l
    }
    assert(initNodes.size === newNodes.size)
  }

  test("withColumns: check no new project addition if redefined alias is not used in" +
    "new columns") {
    val testDf = spark.range(1).select($"id" as "a", $"id" as "b").select($"a" + 1 as "a",
    $"b")
    val initNodes = testDf.queryExecution.logical.collect {
      case l => l
    }

    val newDf = testDf.withColumns(Seq("newCol1"), Seq(col("b") + 2))

    val newNodes = newDf.queryExecution.logical.collect {
      case l => l
    }
    assert(initNodes.size === newNodes.size)
  }

  test("withColumns: new project addition if redefined alias is used in new columns") {
    val testDf = spark.range(1).select($"id" as "a", $"id" as "b").select($"a" + 1 as "a",
      $"b")
    val initNodes = testDf.queryExecution.logical.collect {
      case l => l
    }

    val newDf = testDf.withColumns(Seq("newCol1"), Seq(col("a") + 2))

    val newNodes = newDf.queryExecution.logical.collect {
      case l => l
    }
    assert(newNodes.size === initNodes.size + 1)
  }

  test("withColumns: remap of column should result in project addition") {
    val testDf = spark.range(1).select($"id" as "a", $"id" as "b")
    val initNodes = testDf.queryExecution.logical.collect {
      case l => l
    }

    val newDf = testDf.withColumnRenamed("a", "c")

    val newNodes = newDf.queryExecution.logical.collect {
      case l => l
    }
    assert(newNodes.size === initNodes.size)
  }
}

