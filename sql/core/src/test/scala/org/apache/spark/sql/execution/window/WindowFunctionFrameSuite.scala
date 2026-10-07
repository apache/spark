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

package org.apache.spark.sql.execution.window

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Tests for the window partition-size guard. Window execution tracks the per-row index and window
 * bounds (and the spill-backed sorter's row accounting) in 32-bit `Int`s, so a partition larger
 * than `Int.MaxValue` rows is rejected with a clear error instead of silently producing wrong
 * results. See `WindowEvaluatorFactoryBase.checkPartitionSizeLimit`.
 */
class WindowFunctionFrameSuite extends SparkFunSuite {

  /** Minimal factory exposing the protected partition-size guard for testing. */
  private class TestFactory extends WindowEvaluatorFactoryBase {
    override def windowExpression: Seq[NamedExpression] = Nil
    override def partitionSpec: Seq[Expression] = Nil
    override def orderSpec: Seq[SortOrder] = Nil
    override def childOutput: Seq[Attribute] = Nil
    override def spillSize: SQLMetric = null

    def check(numRows: Long): Unit = checkPartitionSizeLimit(numRows)
  }

  private val maxRows = Int.MaxValue.toLong

  test("partitions up to Int.MaxValue rows are allowed") {
    val factory = new TestFactory
    factory.check(0L)
    factory.check(maxRows)
  }

  test("partitions larger than Int.MaxValue rows are rejected with a clear error") {
    val factory = new TestFactory
    val e = intercept[SparkException] {
      factory.check(maxRows + 1)
    }
    assert(e.getCondition == "WINDOW_FUNCTION_PARTITION_SIZE_EXCEEDS_LIMIT")
    assert(e.getMessageParameters.get("numRows") == (maxRows + 1).toString)

    // Far above the limit is rejected too.
    intercept[SparkException] {
      factory.check(Long.MaxValue)
    }
  }
}
