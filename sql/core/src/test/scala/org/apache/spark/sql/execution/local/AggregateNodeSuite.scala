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

package org.apache.spark.sql.execution.local

import org.apache.spark.sql.catalyst.expressions._

class AggregateNodeSuite extends LocalNodeTest {

  private def aggregateSuite(hasGroupBy: Boolean): Unit = {
    val suiteName = if (hasGroupBy) "with-groupBy" else "without-groupBy"
    test(suiteName) {
      val inputData = {
        for (key <- (0 until 4);
             value <- (0 until 10))
          yield (key, key + value)
      }
      val inputNode = new DummyNode(kvIntAttributes, inputData)
      val keyColumn = inputNode.output(0)
      val valueColumn = inputNode.output(1)
      val groupBy = if (hasGroupBy) Seq(keyColumn) else Nil
      val aggregateNode = AggregateNode(conf, groupBy, Seq(
        Alias(Max(valueColumn), "max")(),
        Alias(Min(valueColumn), "min")(),
        Alias(Sum(valueColumn), "sum")()
      ), inputNode)

      val expectedOutput = if (hasGroupBy) {
        inputData
          .groupBy(_._1)
          .map { case (k, v) => (k, v.map(_._2)) }
          .map { case (k, vs) => (vs.max, vs.min, vs.sum) }
          .toSeq
      } else {
        val results = (inputData.map(_._2).max, inputData.map(_._2).min, inputData.map(_._2).sum)
        Seq(results)
      }
      val actualOutput = aggregateNode.collect().map { row =>
        (row.getInt(0), row.getInt(1), row.getInt(2))
      }
      assert(actualOutput.sorted === expectedOutput.sorted)
    }
  }

  aggregateSuite(hasGroupBy = true)
  aggregateSuite(hasGroupBy = false)
}
