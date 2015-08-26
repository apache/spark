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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.InterpretedMutableProjection
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.unsafe.memory.TaskMemoryManager

class TungstenAggregationIteratorSuite extends SparkFunSuite with SharedSQLContext {

  test("memory acquired on construction") {
    val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.executorMemoryManager)
    val taskContext = new TaskContextImpl(0, 0, 0, 0, taskMemoryManager, null, Seq.empty)
    TaskContext.setTaskContext(taskContext)

    // Assert that a page is allocated before processing starts
    var iter: TungstenAggregationIterator = null
    try {
      val newMutableProjection = (expr: Seq[Expression], schema: Seq[Attribute]) => {
        () => new InterpretedMutableProjection(expr, schema)
      }
      val dummyAccum = SQLMetrics.createLongMetric(ctx.sparkContext, "dummy")
      iter = new TungstenAggregationIterator(Seq.empty, Seq.empty, Seq.empty, 0,
        Seq.empty, newMutableProjection, Seq.empty, None, dummyAccum, dummyAccum)
      val numPages = iter.getHashMap.getNumDataPages
      assert(numPages === 1)
    } finally {
      // Clean up
      if (iter != null) {
        iter.free()
      }
      TaskContext.unset()
    }
  }
}
